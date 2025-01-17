package main

import (
	"context"
	"log"

	"skeleton/internal/application"
	"skeleton/internal/configuration"
	"skeleton/internal/factories"
	"skeleton/internal/repositories/service"
)

func main() {
	ctx := context.Background()

	cfg, err := configuration.New()
	if err != nil {
		log.Fatal(err)
	}

	saFactory := &factories.StatsAnalyzerFactory{}
	sa, err := saFactory.New(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}

	app, err := application.New(ctx, []service.Service{sa})
	if err != nil {
		log.Fatal(err)
	}

	if err := app.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
