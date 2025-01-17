package application

import (
	"context"
	"os"
	"os/signal"
	"skeleton/internal/repositories/service"
	"syscall"
)

type App struct {
	Services         []service.Service
	ServicesWithDown []service.ServiceWithDown
}

func New(
	ctx context.Context,
	services []service.Service,
	servicesWithDown []service.ServiceWithDown,
) (*App, error) {
	return &App{
		Services:         services,
		ServicesWithDown: servicesWithDown,
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
	for _, s := range it.ServicesWithDown {
		s.Down(cxt)
	}

	return nil
}
