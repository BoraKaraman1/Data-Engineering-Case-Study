package main

import (
	"net/http"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	Produced       *prometheus.CounterVec
	Duplicates     prometheus.Counter
	OutOfOrder     prometheus.Counter
	Errors         prometheus.Counter
	ActiveSessions prometheus.Gauge
}

func NewMetrics() *Metrics {
	return &Metrics{
		Produced: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "simulator_events_produced_total",
			Help: "Events enqueued to Kafka by type (includes injected duplicates).",
		}, []string{"event_type"}),
		Duplicates: promauto.NewCounter(prometheus.CounterOpts{
			Name: "simulator_duplicates_injected_total",
			Help: "Events deliberately re-sent to exercise downstream dedup.",
		}),
		OutOfOrder: promauto.NewCounter(prometheus.CounterOpts{
			Name: "simulator_out_of_order_injected_total",
			Help: "Events deliberately delayed to arrive out of timestamp order.",
		}),
		Errors: promauto.NewCounter(prometheus.CounterOpts{
			Name: "simulator_produce_errors_total",
			Help: "Kafka produce errors.",
		}),
		ActiveSessions: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "simulator_active_sessions",
			Help: "Currently active charging sessions across all stations.",
		}),
	}
}

// StartMetricsServer exposes /metrics in the background.
func StartMetricsServer(listen string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: listen, Handler: mux}
	go func() { _ = srv.ListenAndServe() }()
}

// sessionCounter tracks active sessions with an atomic counter mirrored to a gauge.
type sessionCounter struct {
	n int64
	g prometheus.Gauge
}

func (c *sessionCounter) start() { c.g.Set(float64(atomic.AddInt64(&c.n, 1))) }
func (c *sessionCounter) stop()  { c.g.Set(float64(atomic.AddInt64(&c.n, -1))) }
