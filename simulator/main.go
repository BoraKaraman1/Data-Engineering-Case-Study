package main

import (
	"context"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"
)

func main() {
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "config/simulator.yaml"
	}
	cfg, err := LoadConfig(configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	g := newRNG(cfg.Simulator.Seed, cfg)
	stations := BuildStations(cfg, g)
	log.Printf("built %d stations, %d connectors", len(stations), countConnectors(stations))

	// One-shot seeder mode. The registry-seed service runs this image with SEED_ONLY so
	// the roster lands in Postgres (transactionally) BEFORE the processor and simulator
	// start. This closes the stale-registry race on reused volumes: the processor waits
	// on this service completing, so it can never load an old roster mid-reseed.
	if os.Getenv("SEED_ONLY") != "" {
		if err := seedWithRetry(cfg.Postgres.DSN, stations, 30); err != nil {
			log.Fatalf("registry seed failed: %v", err)
		}
		log.Printf("registry seeded (%d stations); exiting (SEED_ONLY)", len(stations))
		return
	}

	if cfg.Postgres.SeedRegistry && os.Getenv("SKIP_SEED") == "" {
		if err := seedWithRetry(cfg.Postgres.DSN, stations, 10); err != nil {
			log.Printf("WARNING: registry seed failed, continuing without it: %v", err)
		} else {
			log.Printf("seeded station/connector registry into Postgres")
		}
	}

	metrics := NewMetrics()
	StartMetricsServer(cfg.Metrics.Listen)
	log.Printf("metrics on %s/metrics", cfg.Metrics.Listen)

	producer := NewProducer(cfg, metrics)
	defer producer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		log.Printf("shutdown signal received, draining...")
		cancel()
	}()

	log.Printf("simulating: accel=%.0fx, cap=%d ev/s, dup=%.0f%%, ooo=%.0f%%",
		cfg.Simulator.TimeAcceleration, cfg.Simulator.TargetEventsPerSec,
		cfg.Simulator.DuplicateRate*100, cfg.Simulator.OutOfOrderRate*100)

	runSimulation(ctx, cfg, stations, producer, metrics)
	log.Printf("simulator stopped")
}

func countConnectors(stations []*Station) int {
	n := 0
	for _, s := range stations {
		n += len(s.Connectors)
	}
	return n
}

func seedWithRetry(dsn string, stations []*Station, attempts int) error {
	var err error
	for i := 0; i < attempts; i++ {
		if err = SeedRegistry(dsn, stations); err == nil {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return err
}

// runSimulation derives a simulated clock from wall time * acceleration and steps
// each station group on a fixed wall tick. Deriving sim-time from wall-time means
// the clock never drifts and back-pressure (a blocked producer) simply causes
// missed ticks to collapse rather than burst.
func runSimulation(ctx context.Context, cfg *Config, stations []*Station, p *Producer, m *Metrics) {
	accel := cfg.Simulator.TimeAcceleration
	startWall := time.Now()
	base := time.Now().UTC()
	simNow := func() time.Time {
		return base.Add(time.Duration(float64(time.Since(startWall)) * accel))
	}

	counter := &sessionCounter{g: m.ActiveSessions}
	initSchedules(stations, simNow(), cfg, rand.New(rand.NewSource(cfg.Simulator.Seed+7)))
	emitInitialStatuses(ctx, stations, simNow(), p)

	workers := runtime.NumCPU()
	if workers < 2 {
		workers = 2
	}
	groups := partition(stations, workers)

	var wg sync.WaitGroup
	for i, group := range groups {
		wg.Add(1)
		go func(group []*Station, workerSeed int64) {
			defer wg.Done()
			wrng := newRNG(workerSeed, cfg)
			last := simNow()
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
				now := simNow()
				dt := now.Sub(last)
				if dt <= 0 {
					continue
				}
				last = now
				stepGroup(ctx, group, now, dt, cfg, p, wrng, counter)
			}
		}(group, cfg.Simulator.Seed+int64(i*1000+13))
	}
	wg.Wait()
}

// initSchedules staggers the first session and heartbeat per station/connector so
// the whole fleet does not fire in lockstep at t=0.
func initSchedules(stations []*Station, now time.Time, cfg *Config, r *rand.Rand) {
	for _, st := range stations {
		st.nextHeartbeatAt = now.Add(time.Duration(r.Intn(30)) * time.Second)
		for _, c := range st.Connectors {
			c.status = "Available"
			c.idleUntil = now.Add(time.Duration(r.Intn(300)) * time.Second) // 0-5 sim-min
		}
	}
}

// emitInitialStatuses gives every connector a defined starting state (Available) so
// the analytics uptime timeline (A2) has a boundary at t=0 instead of an unknown
// first segment. One-time burst at startup; Kafka absorbs it.
func emitInitialStatuses(ctx context.Context, stations []*Station, now time.Time, p *Producer) {
	for _, st := range stations {
		for _, c := range st.Connectors {
			p.Send(ctx, st.statusChange(c, now, "Available"))
		}
	}
}

// stepGroup advances every connector in the group by one tick of simulated time.
func stepGroup(ctx context.Context, group []*Station, now time.Time, dt time.Duration,
	cfg *Config, p *Producer, g *rng, counter *sessionCounter) {

	dtHours := dt.Hours()
	faultProb := cfg.Simulator.FaultRatePerHour * dtHours // per connector this tick

	for _, st := range group {
		if now.After(st.nextHeartbeatAt) {
			p.Send(ctx, st.heartbeat(now))
			st.nextHeartbeatAt = now.Add(time.Duration(28+g.r.Intn(5)) * time.Second)
		}

		for _, c := range st.Connectors {
			switch c.status {
			case "Faulted":
				if now.After(c.faultedUntil) {
					c.status = "Available"
					c.idleUntil = now.Add(idleGap(now, cfg, g))
					p.Send(ctx, st.statusChange(c, now, "Available"))
				}

			case "Available":
				if g.r.Float64() < faultProb {
					p.Send(ctx, st.faultAlert(c, now, g))
					c.status = "Faulted"
					c.faultedUntil = now.Add(downtime(g))
					p.Send(ctx, st.statusChange(c, now, "Faulted"))
					continue
				}
				if now.After(c.idleUntil) {
					p.Send(ctx, st.startSession(c, now, g))
					counter.start()
					p.Send(ctx, st.statusChange(c, now, "Charging"))
				}

			case "Charging":
				if g.r.Float64() < faultProb {
					// fault aborts the session: emit the fault, close out, go down
					p.Send(ctx, st.faultAlert(c, now, g))
					p.Send(ctx, st.stopSession(c, now))
					counter.stop()
					c.status = "Faulted"
					c.faultedUntil = now.Add(downtime(g))
					p.Send(ctx, st.statusChange(c, now, "Faulted"))
					continue
				}
				s := c.session
				if now.After(s.EndsAt) || s.Soc >= 100 {
					p.Send(ctx, st.stopSession(c, now))
					counter.stop()
					c.idleUntil = now.Add(idleGap(now, cfg, g))
					p.Send(ctx, st.statusChange(c, now, "Available"))
					continue
				}
				if now.After(c.nextMeterAt) {
					p.Send(ctx, st.meterTick(c, now, g))
				}
			}
		}
	}
}

// idleGap is the time a connector sits Available between sessions, drawn from an
// exponential whose mean shrinks during peak windows (more arrivals at peak).
func idleGap(now time.Time, cfg *Config, g *rng) time.Duration {
	w := timeWeight(now.Hour(), cfg)
	if w <= 0 {
		w = 1
	}
	baseMeanMin := 8.0
	meanMin := baseMeanMin / w
	mins := expDraw(g.r, meanMin)
	if mins < 0.25 {
		mins = 0.25
	}
	return time.Duration(mins * float64(time.Minute))
}

func downtime(g *rng) time.Duration {
	mins := 5 + g.r.Float64()*25 // 5-30 sim-minutes down
	return time.Duration(mins * float64(time.Minute))
}

// timeWeight returns the arrival multiplier for the given hour: the matching peak
// window's weight, else the base weight.
func timeWeight(hour int, cfg *Config) float64 {
	for _, w := range cfg.Simulator.PeakWindows {
		if hour >= w.Start && hour < w.End {
			return w.Weight
		}
	}
	if cfg.Simulator.BaseArrivalWeight > 0 {
		return cfg.Simulator.BaseArrivalWeight
	}
	return 1.0
}

func expDraw(r *rand.Rand, mean float64) float64 {
	u := r.Float64()
	if u <= 0 {
		u = 1e-9
	}
	return -mean * math.Log(u)
}

// partition splits stations into n roughly equal contiguous groups.
func partition(stations []*Station, n int) [][]*Station {
	if n < 1 {
		n = 1
	}
	if n > len(stations) {
		n = len(stations)
	}
	groups := make([][]*Station, 0, n)
	size := (len(stations) + n - 1) / n
	for i := 0; i < len(stations); i += size {
		end := i + size
		if end > len(stations) {
			end = len(stations)
		}
		groups = append(groups, stations[i:end])
	}
	return groups
}
