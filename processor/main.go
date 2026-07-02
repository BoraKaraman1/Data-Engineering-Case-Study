package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "config/processor.yaml"
	}
	cfg, err := LoadConfig(configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	// The simulator seeds Postgres concurrently, so poll until the roster is present.
	reg, err := LoadRegistryWithRetry(cfg.Postgres.DSN, 90*time.Second)
	if err != nil {
		log.Fatalf("registry: %v", err)
	}
	log.Printf("registry loaded: %d stations, %d tariffs", reg.Len(), len(reg.snap.Load().tariffs))

	// Two clients for two instances with opposite eviction policies: state must persist
	// (noeviction), dedup may be LRU-evicted. See docker-compose.yml for the why.
	stateRdb := redis.NewClient(&redis.Options{Addr: cfg.Redis.Addr})
	if err := pingRedis(stateRdb); err != nil {
		log.Fatalf("redis (state): %v", err)
	}
	defer stateRdb.Close()

	dedupRdb := redis.NewClient(&redis.Options{Addr: cfg.Redis.DedupAddr})
	if err := pingRedis(dedupRdb); err != nil {
		log.Fatalf("redis (dedup): %v", err)
	}
	defer dedupRdb.Close()

	m := NewMetrics()
	StartMetricsServer(cfg.Metrics.Listen, cfg.Metrics.Pprof)
	log.Printf("metrics on %s/metrics (pprof=%v)", cfg.Metrics.Listen, cfg.Metrics.Pprof)

	writers := NewWriters(cfg)
	defer writers.Close()

	dedup := NewDeduper(dedupRdb, time.Duration(cfg.Dedup.TTLSec)*time.Second)
	state := NewStateStore(stateRdb, time.Duration(cfg.State.TTLSec)*time.Second)

	rt := &realtimeHandler{reg: reg, state: state, m: m}
	an := &analyticsHandler{reg: reg, dedup: dedup, writers: writers, m: m}

	ctx, cancel := context.WithCancel(context.Background())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		log.Printf("shutdown signal received, draining...")
		cancel()
	}()

	// Pick up new stations / tariff changes without a restart (0 disables).
	if n := *cfg.Registry.RefreshSec; n > 0 {
		log.Printf("registry refresh: every %ds", n)
		go StartRegistryRefresher(ctx, reg, cfg.Postgres.DSN, time.Duration(n)*time.Second)
	}

	log.Printf("starting groups: realtime=%d analytics=%d on %q",
		cfg.Workers.Realtime, cfg.Workers.Analytics, cfg.Kafka.TopicRaw)
	log.Printf("analytics batching: batch_size=%d flush_ms=%d", cfg.Analytics.BatchSize, cfg.Analytics.FlushMs)
	log.Printf("realtime batching: batch_max_messages=%d batch_max_wait_ms=%d",
		cfg.Realtime.BatchMaxMessages, cfg.Realtime.BatchMaxWaitMs)

	var wg sync.WaitGroup
	runRealtimeBatchGroup(ctx, &wg, cfg, m, realtimeBatchSpec{
		name: "realtime", groupID: cfg.Kafka.GroupRealtime,
		workers: cfg.Workers.Realtime, apply: rt.applyBatch,
	})
	runAnalyticsBatchGroup(ctx, &wg, cfg, m, batchGroupSpec{
		name: "analytics", groupID: cfg.Kafka.GroupAnalytics,
		workers: cfg.Workers.Analytics, flush: an.flush,
	})

	<-ctx.Done()
	wg.Wait()
	log.Printf("processor stopped")
}

func pingRedis(rdb *redis.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var err error
	for i := 0; i < 10; i++ {
		if err = rdb.Ping(ctx).Err(); err == nil {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return err
}
