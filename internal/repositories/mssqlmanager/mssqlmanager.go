package mssqlmanager

import (
	"skeleton/internal/repositories/service"

	"github.com/jmoiron/sqlx"
)

type MSSQLManagerRepo interface {
	service.ServiceWithDown

	// Here you can define your own methods.
	GetDB() *sqlx.DB
}
