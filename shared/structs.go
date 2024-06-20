package shared

import "database/sql"

type RawColumnDetails struct {
	ColumnName    string
	DataType      string
	ColumnType    string
	ColumnKey     string
	IsNullable    string
	ColumnDefault sql.NullString
	Extra         string
}
