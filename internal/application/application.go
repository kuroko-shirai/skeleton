package application

import (
	"context"
	"os"
	"os/signal"
	"skeleton/internal/repositories/service"
	"syscall"
)

type App struct {
	Services []service.Service
}

func New(ctx context.Context, services []service.Service) (*App, error) {
	return &App{
		Services: services,
	}, nil
}

func (it *App) Run(ctx context.Context) error {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	for _, s := range it.Services {
		go s.Up(ctx)
	}

	select {
	case <-sigChan:
		return it.Down(ctx)
	case <-ctx.Done():
		return it.Down(ctx)
	}
}

func (it *App) Down(cxt context.Context) error {
	for _, s := range it.Services {
		s.Down(cxt)
	}

	return nil
}
