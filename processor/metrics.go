package main

import (
	"net/http"
	"net/http/pprof"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics are the processor's Prometheus signals. The wall-clock transport lag is the
// Phase-4 SLO (acceleration-immune); ingestion_lag (ingested_at - event_time) is kept
// but only meaningful at time_acceleration=1.
type Metrics struct {
	Consumed          *prometheus.CounterVec
	CleanProduced     prometheus.Counter
	DLQ               *prometheus.CounterVec
	DuplicatesDropped prometheus.Counter
	ValidationErrors  *prometheus.CounterVec
	StaleSkipped      prometheus.Counter
	RedisErrors       *prometheus.CounterVec
	TransportLag      prometheus.Histogram
	IngestionLag      prometheus.Histogram
	RedisWrite        *prometheus.HistogramVec
	CleanProduce      prometheus.Histogram
	ConsumerLag       *prometheus.GaugeVec
}

func NewMetrics() *Metrics {
	subSecond := []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}
	redisB := []float64{.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1}
	produceB := []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1}
	return &Metrics{
		Consumed: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "processor_events_consumed_total",
			Help: "Events fetched from the raw topic by consumer group and type.",
		}, []string{"group", "event_type"}),
		CleanProduced: promauto.NewCounter(prometheus.CounterOpts{
			Name: "processor_clean_produced_total",
			Help: "Validated, deduped events written to the clean topic.",
		}),
		DLQ: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "processor_dlq_total",
			Help: "Events routed to the dead-letter topic by reason.",
		}, []string{"reason"}),
		DuplicatesDropped: promauto.NewCounter(prometheus.CounterOpts{
			Name: "processor_duplicates_dropped_total",
			Help: "In-window duplicates dropped by the Redis dedup optimization.",
		}),
		ValidationErrors: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "processor_validation_errors_total",
			Help: "Validation failures by rule.",
		}, []string{"rule"}),
		StaleSkipped: promauto.NewCounter(prometheus.CounterOpts{
			Name: "processor_state_stale_skipped_total",
			Help: "Real-time state writes skipped because a newer event already applied.",
		}),
		RedisErrors: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "processor_redis_errors_total",
			Help: "Redis operation errors by op (dedup, state).",
		}, []string{"op"}),
		TransportLag: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "processor_transport_lag_seconds",
			Help:    "Wall-clock lag from Kafka produce time to processing (acceleration-immune SLO).",
			Buckets: subSecond,
		}),
		IngestionLag: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "processor_ingestion_lag_seconds",
			Help:    "Event-time to ingest lag; only meaningful at time_acceleration=1.",
			Buckets: subSecond,
		}),
		RedisWrite: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "processor_redis_write_seconds",
			Help:    "Redis round-trip latency by op (dedup, state).",
			Buckets: redisB,
		}, []string{"op"}),
		CleanProduce: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "processor_clean_produce_seconds",
			Help:    "Latency of a synchronous clean-topic produce.",
			Buckets: produceB,
		}),
		ConsumerLag: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "processor_consumer_lag",
			Help: "Best-effort consumer lag per group from kafka-go reader stats.",
		}, []string{"group"}),
	}
}

// StartMetricsServer exposes /metrics in the background. pprof is gated behind a config
// flag because /debug/pprof is sensitive: on for local Phase-4 profiling, off in prod.
func StartMetricsServer(listen string, enablePprof bool) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	if enablePprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	srv := &http.Server{Addr: listen, Handler: mux}
	go func() { _ = srv.ListenAndServe() }()
}
