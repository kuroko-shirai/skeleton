package userservice

import (
	"context"
	"log"
	"skeleton/internal/configuration"
	"skeleton/internal/repositories/mssqlmanager"
	"time"

	"github.com/jmoiron/sqlx"
)

type UserService struct {
	db *sqlx.DB
}

func New(
	ctx context.Context,
	cfg *configuration.Configuration,
	dm mssqlmanager.MSSQLManagerRepo,
) (*UserService, error) {
	return &UserService{db: dm.GetDB()}, nil
}

func (it *UserService) Up(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	default:
		return it.process(ctx)
	}
}

func (it *UserService) Down(ctx context.Context) error {
	return nil
}

func (it *UserService) DoSomething() error {
	log.Println("ping")

	return nil
}

func (it *UserService) process(_ context.Context) error {
	for {
		time.Sleep(time.Second)

		it.DoSomething()
	}
}
