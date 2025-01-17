package prometheus

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// QueryMetrics is a struct that allows to write metrics of count and latency of dbms queries
type QueryMetrics struct {
	queries *prometheus.CounterVec
	latency *prometheus.HistogramVec
}

func NewQueryMetrics(service, host string) *QueryMetrics {
	queriesCollector := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "queries_count",
			Help:        "How many queries processed",
			ConstLabels: prometheus.Labels{"app": service, "host": host},
		},
		[]string{"query", "success"},
	)

	latencyCollector := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "queries_latency",
		Help:        "How long it took to process the query",
		ConstLabels: prometheus.Labels{"app": service, "host": host},
		Buckets:     []float64{200, 300, 400, 500, 600, 700, 800, 900, 1000, 1200, 1500, 2000},
	},
		[]string{"query", "success"},
	)

	prometheus.MustRegister(queriesCollector, latencyCollector)

	return &QueryMetrics{
		queries: queriesCollector,
		latency: latencyCollector,
	}
}

// Inc increases the counter for the given "query" and "success" fields by 1
func (h *QueryMetrics) Inc(query string, success bool) {
	h.queries.WithLabelValues(query, strconv.FormatBool(success)).Inc()
}

// WriteTiming writes time elapsed since the startTime.
// for the given "query" and "success" fields
func (h *QueryMetrics) WriteTiming(startTime time.Time, query string, success bool) {
	h.latency.WithLabelValues(query, strconv.FormatBool(success)).Observe(timeFromStart(startTime))
}

func (h *QueryMetrics) WriteTimingAndCounter(startTime time.Time, query string, success bool) {
	successStr := strconv.FormatBool(success)
	h.queries.WithLabelValues(query, successStr).Inc()
	h.latency.WithLabelValues(query, successStr).Observe(timeFromStart(startTime))
}

func timeFromStart(start time.Time) float64 {
	return float64(time.Since(start).Milliseconds())
}
