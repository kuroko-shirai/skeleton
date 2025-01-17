package mssql

import (
	mssql "github.com/denisenkom/go-mssqldb"
)

func TVP(tableType string, items any) mssql.TVP {
	//tableType тип табличной переменной
	//items слайс значений табличной переменной
	return mssql.TVP{
		TypeName: tableType,
		Value:    items,
	}
}
