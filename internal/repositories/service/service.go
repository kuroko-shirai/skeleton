package service

import (
	"context"
)

// Service: used for any services which should be running.
type Service interface {
	Up(context.Context) error
}

// ServiceWithDown: used for any services which should be
// running and safly ended.
type ServiceWithDown interface {
	Service

	Down(context.Context) error
}
