package mssqlmanager

import (
	"context"
	"database/sql"
	"fmt"
	"skeleton/internal/configuration"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/jmoiron/sqlx"
)

type MSSQLManager struct {
	storage *sqlx.DB
}

func New(
	ctx context.Context,
	cfg *configuration.Configuration,
) (*MSSQLManager, error) {
	newDSN := fmt.Sprintf(
		"sqlserver://%s:%s@%s:%s?database=%s",
		cfg.MSSQL.Username,
		cfg.MSSQL.Password,
		cfg.MSSQL.Address,
		cfg.MSSQL.Port,
		cfg.MSSQL.Database,
	)

	newDriver := "sqlserver"

	newConnection, err := sql.Open(newDriver, newDSN)
	if err != nil {
		return nil, err
	}

	return &MSSQLManager{
		storage: sqlx.NewDb(newConnection, newDriver),
	}, nil
}

func (it *MSSQLManager) Up(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	default:
		return it.process(ctx)
	}
}

func (it *MSSQLManager) process(_ context.Context) error {
	for {
		//time.Sleep(time.Second)

		//log.Println("ping")
	}
}

func (it *MSSQLManager) Down(ctx context.Context) error {
	return it.storage.Close()
}

func (it *MSSQLManager) GetDB() *sqlx.DB {
	return it.storage
}
