package configuration

import (
	"skeleton/pkg/mssql"
	"skeleton/pkg/prometheus"
	"skeleton/pkg/redis"

	"github.com/ilyakaznacheev/cleanenv"
)

const (
	_path = "./config.yaml"
)

type Configuration struct {
	// TODO: Here place configurations of your services.

	// Here defined default configurations. Please, put your
	// configurations in the same format.
	REDIS      redis.Config      `yaml:"redis"`
	MSSQL      mssql.Config      `yaml:"mssql"`
	Prometheus prometheus.Config `yaml:"prometheus"`
	// Jaeger     jaeger.Config     `yaml:"jaeger"`
}

func New() (*Configuration, error) {
	var config Configuration

	if err := cleanenv.ReadConfig(_path, &config); err != nil {
		return nil, err
	}

	return &config, nil
}
