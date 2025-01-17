package prometheus

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Config struct {
	Service string `env:"PROMETHEUS_SERVICE" yaml:"service"`
	Host    string `env:"PROMETHEUS_HOST" yaml:"host"`
	Port    string `env:"PROMETHEUS_PORT" yaml:"port"`
	Enabled bool   `env:"PROMETHEUS_METRICS" env-default:"false" yaml:"enabled"`
}

func Init(config *Config) {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(":"+config.Port, nil); err != nil {
			log.Printf("Ошибка инициализации prometheus %s", err.Error())
		}
	}()
}
