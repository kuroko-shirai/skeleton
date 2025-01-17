package mssql

import (
	"fmt"
	"time"
)

const (
	_defaultConnectionMask             = "server=%s;user id=%s;password=%s;port=%s;"
	_defaultConnectionMaskWithDatabase = "server=%s;user id=%s;password=%s;port=%s;database=%s;"
)

type Config struct {
	Name           string        `env:"MSSQL_NAME"  yaml:"name"`
	Driver         string        `env:"MSSQL_DRIVER"  yaml:"driver"`
	Address        string        `env:"MSSQL_ADDRESS" yaml:"address"`
	Port           string        `env:"MSSQL_PORT" yaml:"port"`
	Database       string        `env:"MSSQL_DATABASE" yaml:"database"`
	Username       string        `env:"MSSQL_USERNAME" yaml:"username"`
	Password       string        `env:"MSSQL_PASSWORD" yaml:"password"`
	Timeout        time.Duration `env:"MSSQL_TIMEOUT" yaml:"timeout"`
	RetryLimit     int           `env:"MSSQL_RETRY_LIMIT" yaml:"retry_limit"`
	MaxPoolConnect int           `env:"MSSQL_MAX_POOL_CONNECT" yaml:"max_pool_connect"`
}

func (c *Config) ConnectionString() string {
	newConnectionString := fmt.Sprintf(
		_defaultConnectionMask,
		c.Address,
		c.Username,
		c.Password,
		c.Port,
	)
	if c.Database != "" {
		newConnectionString = fmt.Sprintf(
			_defaultConnectionMaskWithDatabase,
			c.Address,
			c.Username,
			c.Password,
			c.Port,
			c.Database,
		)
	}
	return newConnectionString
}
