package statsanalyzer

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"skeleton/internal/configuration"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/jmoiron/sqlx"
)

type StatsAnalyzer struct {
	storage *sqlx.DB
}

func New(
	ctx context.Context,
	cfg *configuration.Configuration,
) (*StatsAnalyzer, error) {
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

	return &StatsAnalyzer{
		storage: sqlx.NewDb(newConnection, newDriver),
	}, nil
}

func (it *StatsAnalyzer) Up(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	default:
		return it.process(ctx)
	}
}

func (it *StatsAnalyzer) Down(ctx context.Context) error {
	return it.storage.Close()
}

const (
	_sql = `
	SELECT 
		FORMAT(date_add, 'yyyy-MM-dd HH') AS Date,
		COUNT(*) AS Value
	FROM 
		SQL_03.dbo.arc_ost_03_all
	WHERE 
		ost > 0 AND
		ShopNo = 5005 AND
		id_tov = 731
	GROUP BY 
		FORMAT(date_add, 'yyyy-MM-dd HH')
	ORDER BY 
		Date`
)

type Row struct {
	Date  string
	Value string
}

func (it *StatsAnalyzer) process(_ context.Context) error {
	for {
		fmt.Println(">>0")

		result, err := it.storage.Query(_sql)
		if err != nil {
			fmt.Println(">>0 err", err)
			return err
		}

		fmt.Println(">>1")

		rows := make([]Row, 0)
		for result.Next() {
			var row Row
			err := result.Scan(&row.Date, &row.Value)
			if err != nil {
				fmt.Println(">>3 err", err)
				return err
			}
			fmt.Println(">>3")

			rows = append(rows, row)
		}

		log.Println(rows)
	}
}
