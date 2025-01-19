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

	mssqlManagerFactory := &factories.MSSQLManagerFactory{}
	mssqlManager, err := mssqlManagerFactory.New(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}

	redisManagerFactory := &factories.REDISManagerFactory{}
	redisManager, err := redisManagerFactory.New(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}

	userServiceFactory := &factories.UserServiceFactory{}
	userService, err := userServiceFactory.New(ctx, cfg, mssqlManager)
	if err != nil {
		log.Fatal(err)
	}

	app, err := application.New(
		ctx,
		[]service.Service{
			userService,
		},
		[]service.ServiceWithDown{
			mssqlManager,
			redisManager,
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	if err := app.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
