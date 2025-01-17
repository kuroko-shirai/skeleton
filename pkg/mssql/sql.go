package mssql

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
)

const (
	_defaultDriver  = "sqlserver"
	_defaultMaskDSN = "sqlserver://%s:%s@%s:%s?database=%s"
)

type Database struct {
	serverName string
	dbName     string
	metrics    metrics
	connection *sqlx.DB
}

func New(config *Config) (*Database, error) {
	db := new(Database)

	newDSN := fmt.Sprintf(
		_defaultMaskDSN,
		config.Username,
		config.Password,
		config.Address,
		config.Port,
		config.Database,
	)

	newDriver := _defaultDriver
	if config.Driver != "" {
		newDriver = config.Driver
	}

	newConnection, err := sql.Open(newDriver, newDSN)
	if err != nil {
		return nil, fmt.Errorf("Возникли проблемы при открытии подключения к MSSQL базе. Error: %w, Path: %s, Driver: %s", err, newDSN, newDriver)
	}

	db.connection = sqlx.NewDb(newConnection, newDriver)

	db.connection.SetMaxOpenConns(config.MaxPoolConnect)

	if err := db.ping(); err != nil {
		return nil, fmt.Errorf("Возникли проблемы при ping к MSSQL. Error: %w, Path: %s, Driver: %s", err, newDSN, newDriver)
	}

	return db, nil
}

func (d *Database) ping() error {
	if row := d.connection.QueryRowx("SELECT @@SERVERNAME, DB_NAME()"); row != nil {
		if err := row.Scan(&d.serverName, &d.dbName); err != nil {
			return err
		}
	}
	return nil
}

func (d *Database) DB() *sqlx.DB {
	return d.connection
}

func (d *Database) Close() error {
	return d.connection.Close()
}
