package prometheus

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
)

type CountMetrics struct {
	counter prometheus.Counter
}

func NewCountMetrics(service, host string) *CountMetrics {
	counter := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name:        "queries_count",
			Help:        "How many queries processed",
			ConstLabels: prometheus.Labels{"app": service, "host": host},
		},
	)

	prometheus.MustRegister(counter)

	return &CountMetrics{
		counter: counter,
	}
}

// Inc increases the counter for the given "query" and "success" fields by 1
func (h *CountMetrics) Inc(query string, success bool) {
	h.counter.Inc()
}

func NewCustomCountMetrics(name, service string) *CountMetrics {
	host, _ := os.Hostname()
	counter := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name:        name,
			ConstLabels: prometheus.Labels{"app": service, "host": host},
		},
	)

	prometheus.MustRegister(counter)

	return &CountMetrics{
		counter: counter,
	}
}
