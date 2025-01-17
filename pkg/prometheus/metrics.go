package prometheus

import (
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var DefaultBucket = []float64{200, 300, 400, 500, 600, 700, 800, 900, 1000, 1200, 1500, 2000}

type Metrics struct {
	requestCount *prometheus.CounterVec
	latency      *prometheus.HistogramVec
}

func NewMetrics(service string, bucket []float64, labels []string) *Metrics {
	host, _ := os.Hostname()
	queriesCollector := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "request_count",
			Help:        "How many request processed",
			ConstLabels: prometheus.Labels{"app": service, "instance": host},
		}, labels,
	)

	latencyCollector := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "request_time",
		Help:        "How long it took to process the request",
		ConstLabels: prometheus.Labels{"app": service, "instance": host},
		Buckets:     bucket,
	}, labels,
	)

	prometheus.MustRegister(queriesCollector, latencyCollector)

	return &Metrics{
		requestCount: queriesCollector,
		latency:      latencyCollector,
	}
}

func (h *Metrics) Inc(labelValues []string) {
	if h == nil {
		return
	}
	h.requestCount.WithLabelValues(labelValues...).Inc()
}

func (h *Metrics) WriteTiming(startTime time.Time, labelValues []string) {
	if h == nil {
		return
	}
	h.latency.WithLabelValues(labelValues...).Observe(timeFromStart(startTime))
}

func (h *Metrics) WriteTimingAndCounter(startTime time.Time, labelValues []string) {
	if h == nil {
		return
	}
	h.requestCount.WithLabelValues(labelValues...).Inc()
	h.latency.WithLabelValues(labelValues...).Observe(timeFromStart(startTime))
}
