package service

import (
	"context"
)

type Service interface {
	Up(context.Context) error
	Down(context.Context) error
}
