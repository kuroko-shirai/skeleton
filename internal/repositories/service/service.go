package service

import (
	"context"
)

type Service interface {
	Up(context.Context) error
}

type ServiceWithDown interface {
	Service

	Down(context.Context) error
}
