package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config mirrors config/processor.yaml.
type Config struct {
	Kafka     KafkaConfig     `yaml:"kafka"`
	Redis     RedisConfig     `yaml:"redis"`
	Postgres  PostgresConfig  `yaml:"postgres"`
	Dedup     DedupConfig     `yaml:"dedup"`
	State     StateConfig     `yaml:"state"`
	Metrics   MetricsConfig   `yaml:"metrics"`
	Workers   WorkersConfig   `yaml:"workers"`
	Analytics AnalyticsConfig `yaml:"analytics"`
	Realtime  RealtimeConfig  `yaml:"realtime"`
	Readers   ReadersConfig   `yaml:"readers"`
	Registry  RegistryConfig  `yaml:"registry"`
}

type KafkaConfig struct {
	Brokers        []string `yaml:"brokers"`
	TopicRaw       string   `yaml:"topic_raw"`
	TopicClean     string   `yaml:"topic_clean"`
	TopicDLQ       string   `yaml:"topic_dlq"`
	GroupRealtime  string   `yaml:"group_realtime"`
	GroupAnalytics string   `yaml:"group_analytics"`
	BatchSize      int      `yaml:"batch_size"`
	LingerMs       int      `yaml:"linger_ms"`
}

// RedisConfig points at two instances: Addr is the current-state store (must-persist,
// noeviction) and DedupAddr is the dedup-key store (evictable, allkeys-lru), split so a
// dedup flood can't LRU-evict live state. DedupAddr falls back to Addr when unset, so a
// single-Redis deployment still works.
type RedisConfig struct {
	Addr      string `yaml:"addr"`
	DedupAddr string `yaml:"dedup_addr"`
}

type PostgresConfig struct {
	DSN string `yaml:"dsn"`
}

type DedupConfig struct {
	TTLSec int `yaml:"ttl_sec"`
}

type StateConfig struct {
	TTLSec int `yaml:"ttl_sec"`
}

type MetricsConfig struct {
	Listen string `yaml:"listen"`
	Pprof  bool   `yaml:"pprof"` // expose /debug/pprof (local-only; disable in production)
}

type WorkersConfig struct {
	Realtime  int `yaml:"realtime"`
	Analytics int `yaml:"analytics"`
}

type AnalyticsConfig struct {
	BatchSize int `yaml:"batch_size"`
	FlushMs   int `yaml:"flush_ms"`
}

// RealtimeConfig is the bounded opportunistic micro-batch for the current-state path:
// flush the CAS pipeline after BatchMaxMessages (N) accumulate OR BatchMaxWaitMs (T)
// elapses since the batch's first message, whichever comes first.
type RealtimeConfig struct {
	BatchMaxMessages int `yaml:"batch_max_messages"`
	BatchMaxWaitMs   int `yaml:"batch_max_wait_ms"`
}

// ReadersConfig splits the Kafka reader fetch tuning per consumer group so the latency-first
// realtime group and the throughput-first analytics group no longer share one fetch floor.
type ReadersConfig struct {
	Realtime  readerTuning `yaml:"realtime"`
	Analytics readerTuning `yaml:"analytics"`
}

// readerTuning is the per-group broker-fetch tuning: MinBytes is the fetch-size floor and
// MaxWaitMs caps how long a fetch blocks waiting for that floor. kafka-go defaults MaxWait
// to 10s, which stalls a low-traffic realtime reader's first message far past the freshness
// SLO, so both are made explicit here.
type readerTuning struct {
	MinBytes  int `yaml:"min_bytes"`
	MaxWaitMs int `yaml:"max_wait_ms"`
}

// RegistryConfig controls the periodic registry refresh. RefreshSec is a pointer so an
// explicit 0 (disable) is distinguishable from an absent key (which defaults to 300);
// main.go starts the refresher only when it is > 0.
type RegistryConfig struct {
	RefreshSec *int `yaml:"refresh_sec"`
}

// LoadConfig reads and parses the YAML config, filling in sane defaults.
func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if len(c.Kafka.Brokers) == 0 {
		return nil, fmt.Errorf("kafka.brokers is required")
	}
	if c.Redis.DedupAddr == "" {
		c.Redis.DedupAddr = c.Redis.Addr // single-Redis fallback
	}
	if c.Dedup.TTLSec <= 0 {
		c.Dedup.TTLSec = 120
	}
	if c.State.TTLSec <= 0 {
		c.State.TTLSec = 300 // "current status = last 5 minutes"; key existence == fresh
	}
	if c.Workers.Realtime <= 0 {
		c.Workers.Realtime = 4
	}
	if c.Workers.Analytics <= 0 {
		c.Workers.Analytics = 6
	}
	if c.Kafka.BatchSize <= 0 {
		c.Kafka.BatchSize = 1000
	}
	if c.Analytics.BatchSize <= 0 {
		c.Analytics.BatchSize = 500
	}
	if c.Analytics.FlushMs <= 0 {
		c.Analytics.FlushMs = 100
	}
	if c.Realtime.BatchMaxMessages <= 0 {
		c.Realtime.BatchMaxMessages = 750
	}
	if c.Realtime.BatchMaxWaitMs <= 0 {
		c.Realtime.BatchMaxWaitMs = 25
	}
	if c.Readers.Realtime.MinBytes <= 0 {
		c.Readers.Realtime.MinBytes = 1
	}
	if c.Readers.Realtime.MaxWaitMs <= 0 {
		c.Readers.Realtime.MaxWaitMs = 50
	}
	if c.Readers.Analytics.MinBytes <= 0 {
		c.Readers.Analytics.MinBytes = 10000
	}
	if c.Readers.Analytics.MaxWaitMs <= 0 {
		c.Readers.Analytics.MaxWaitMs = 1000
	}
	if c.Registry.RefreshSec == nil {
		def := 300
		c.Registry.RefreshSec = &def
	}
	if c.Metrics.Listen == "" {
		c.Metrics.Listen = ":9102"
	}
	return &c, nil
}
