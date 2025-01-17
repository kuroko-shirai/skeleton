package userservice

import (
	"context"
	"log"
	"skeleton/internal/configuration"
	"skeleton/internal/repositories/mssqlmanager"
	"time"
)

type UserService struct {
	mssqlManager mssqlmanager.MSSQLManagerRepo
}

func New(
	ctx context.Context,
	cfg *configuration.Configuration,
	mssqlManager mssqlmanager.MSSQLManagerRepo,
) (*UserService, error) {
	return &UserService{mssqlManager: mssqlManager}, nil
}

func (it *UserService) Up(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	default:
		return it.process(ctx)
	}
}

func (it *UserService) DoSomething() error {
	log.Println("ping")

	// Here we can use db connection from MSSQL
	// mssqlManagerdb := it.mssqlManager.GetDB()

	return nil
}

func (it *UserService) process(_ context.Context) error {
	for {
		time.Sleep(time.Second)

		it.DoSomething()
	}
}
