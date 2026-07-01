package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config mirrors config/simulator.yaml.
type Config struct {
	Simulator SimulatorConfig `yaml:"simulator"`
	Kafka     KafkaConfig     `yaml:"kafka"`
	Postgres  PostgresConfig  `yaml:"postgres"`
	Metrics   MetricsConfig   `yaml:"metrics"`
	Cities    []City          `yaml:"cities"`
}

type SimulatorConfig struct {
	StationCount         int          `yaml:"station_count"`
	ConnectorsPerStation [2]int       `yaml:"connectors_per_station"`
	Seed                 int64        `yaml:"seed"`
	TargetEventsPerSec   int          `yaml:"target_events_per_sec"`
	TimeAcceleration     float64      `yaml:"time_acceleration"`
	DuplicateRate        float64      `yaml:"duplicate_rate"`
	OutOfOrderRate       float64      `yaml:"out_of_order_rate"`
	FaultRatePerHour     float64      `yaml:"fault_rate_per_hour"`
	MeterIntervalSec     [2]int       `yaml:"meter_interval_sec"`
	SessionMinutes       SessionDist  `yaml:"session_minutes"`
	PeakWindows          []PeakWindow `yaml:"peak_windows"`
	BaseArrivalWeight    float64      `yaml:"base_arrival_weight"`
	Operators            []string     `yaml:"operators"`
}

type SessionDist struct {
	Mean   float64 `yaml:"mean"`
	Stddev float64 `yaml:"stddev"`
	Min    float64 `yaml:"min"`
	Max    float64 `yaml:"max"`
}

type PeakWindow struct {
	Start  int     `yaml:"start"`
	End    int     `yaml:"end"`
	Weight float64 `yaml:"weight"`
}

type KafkaConfig struct {
	Brokers        []string `yaml:"brokers"`
	TopicRaw       string   `yaml:"topic_raw"`
	Acks           string   `yaml:"acks"`
	BatchSize      int      `yaml:"batch_size"`
	BatchTimeoutMs int      `yaml:"batch_timeout_ms"`
	Async          bool     `yaml:"async"`
}

type PostgresConfig struct {
	DSN          string `yaml:"dsn"`
	SeedRegistry bool   `yaml:"seed_registry"`
}

type MetricsConfig struct {
	Listen string `yaml:"listen"`
}

type City struct {
	Code   string  `yaml:"code"`
	Name   string  `yaml:"name"`
	Lat    float64 `yaml:"lat"`
	Lon    float64 `yaml:"lon"`
	Weight int     `yaml:"weight"`
}

// LoadConfig reads and parses the YAML config at path.
func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if c.Simulator.StationCount <= 0 {
		return nil, fmt.Errorf("station_count must be > 0")
	}
	if len(c.Cities) == 0 {
		return nil, fmt.Errorf("at least one city is required")
	}
	if c.Simulator.TimeAcceleration <= 0 {
		c.Simulator.TimeAcceleration = 1.0
	}
	return &c, nil
}
