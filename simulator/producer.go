package main

import (
	"context"
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"golang.org/x/time/rate"
)

// Producer wraps a kafka.Writer and adds the two reliability hooks the case asks
// us to exercise downstream: at-least-once duplicates and out-of-order arrival.
type Producer struct {
	w       *kafka.Writer
	limiter *rate.Limiter
	metrics *Metrics
	dupRate float64
	oooRate float64

	mu  sync.Mutex
	rnd *rand.Rand

	delay      *delayQueue
	workerOnce sync.Once
}

func NewProducer(cfg *Config, m *Metrics) *Producer {
	acks := kafka.RequireOne
	switch cfg.Kafka.Acks {
	case "all":
		acks = kafka.RequireAll
	case "none":
		acks = kafka.RequireNone
	}

	w := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Kafka.Brokers...),
		Topic:        cfg.Kafka.TopicRaw,
		Balancer:     &kafka.Hash{}, // hash(key=station_id) -> stable partition, per-station order
		BatchSize:    cfg.Kafka.BatchSize,
		BatchTimeout: time.Duration(cfg.Kafka.BatchTimeoutMs) * time.Millisecond,
		RequiredAcks: acks,
		Async:        cfg.Kafka.Async,
		Compression:  kafka.Snappy,
	}

	p := &Producer{
		w:       w,
		metrics: m,
		dupRate: cfg.Simulator.DuplicateRate,
		oooRate: cfg.Simulator.OutOfOrderRate,
		rnd:     rand.New(rand.NewSource(cfg.Simulator.Seed + 1)),
		delay:   newDelayQueue(),
	}
	if cfg.Simulator.TargetEventsPerSec > 0 {
		p.limiter = rate.NewLimiter(rate.Limit(cfg.Simulator.TargetEventsPerSec), cfg.Simulator.TargetEventsPerSec)
	}
	// Async writes report failures here rather than from WriteMessages.
	w.Completion = func(msgs []kafka.Message, err error) {
		if err != nil {
			m.Errors.Add(float64(len(msgs)))
		}
	}
	return p
}

func (p *Producer) chance(prob float64) bool {
	if prob <= 0 {
		return false
	}
	p.mu.Lock()
	v := p.rnd.Float64()
	p.mu.Unlock()
	return v < prob
}

func (p *Producer) randInt(n int) int {
	p.mu.Lock()
	v := p.rnd.Intn(n)
	p.mu.Unlock()
	return v
}

// Send serialises and writes an event, applying the rate cap and injection hooks.
func (p *Producer) Send(ctx context.Context, e Event) {
	if p.limiter != nil {
		if err := p.limiter.Wait(ctx); err != nil {
			return // context cancelled during shutdown
		}
	}
	b, err := json.Marshal(e)
	if err != nil {
		p.metrics.Errors.Inc()
		return
	}
	msg := kafka.Message{Key: []byte(e.StationID), Value: b}

	// Out-of-order: hold this one and release after a short delay so later events
	// overtake it on the partition. Instead of a goroutine+timer per event (thousands
	// could pile up at high ingest), hand it to one shared delay queue drained by a
	// single worker. The worker binds to this Send's ctx; every Send runs under the
	// same simulation ctx, so pending events still drop together on cancellation.
	if p.chance(p.oooRate) {
		p.metrics.OutOfOrder.Inc()
		delay := time.Duration(2000+p.randInt(8000)) * time.Millisecond
		p.workerOnce.Do(func() { go p.delay.worker(ctx, p.write) })
		p.delay.push(delayedMsg{fireAt: time.Now().Add(delay), msg: msg, eventType: e.EventType})
		return
	}

	p.write(ctx, msg, e.EventType)

	// Duplicate: re-send the identical message (same event_id) -> at-least-once.
	if p.chance(p.dupRate) {
		p.metrics.Duplicates.Inc()
		p.write(ctx, msg, e.EventType)
	}
}

func (p *Producer) write(ctx context.Context, msg kafka.Message, eventType string) {
	if err := p.w.WriteMessages(ctx, msg); err != nil {
		p.metrics.Errors.Inc()
		return
	}
	// With Async=true this counts enqueue, not ack; Completion tracks failures.
	p.metrics.Produced.WithLabelValues(eventType).Inc()
}

func (p *Producer) Close() error { return p.w.Close() }
