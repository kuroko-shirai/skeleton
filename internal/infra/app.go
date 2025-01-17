package infra

import (
	"context"
	"os"
	"os/signal"
	"skeleton/internal/config"
	"skeleton/internal/repositories/statsanalyzer"
	"syscall"
)

type Service interface {
	Up(context.Context) error
	Down(context.Context) error
}

type App struct {
	Services []Service
}

func New(ctx context.Context) (*App, error) {
	cfg, err := config.New()
	if err != nil {
		return nil, err
	}

	sa, err := statsanalyzer.New(ctx, cfg)
	if err != nil {
		return nil, err
	}

	return &App{
		Services: []Service{
			sa,
		},
	}, nil
}

func (this *App) Run(ctx context.Context) error {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	for _, s := range this.Services {
		go s.Up(ctx)
	}

	select {
	case <-sigChan:
		return this.Down(ctx)
	case <-ctx.Done():
		return this.Down(ctx)
	}
}

func (this *App) Down(cxt context.Context) error {
	for _, s := range this.Services {
		s.Down(cxt)
	}

	return nil
}
