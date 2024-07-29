package shared

import "database/sql"

type RawColumnDetails struct {
	ColumnName    string
	DataType      string
	ColumnType    string
	IsNullable    string
	ColumnDefault sql.NullString
	MaxLength     sql.NullInt64
	Extra         string
	IsUnique      bool
	IsForeign     bool
	ForeignKey    struct {
		ReferenceTableName  sql.NullString
		ReferenceColumnName sql.NullString
		OnUpdate            sql.NullString
		OnDelete            sql.NullString
	}
	Precision sql.NullInt64
	Scale     sql.NullInt64
}

type ForeignKey struct {
	ColumnName          string
	ReferenceTableName  string
	ReferenceColumnName string
	OnUpdate            string
	OnDelete            string
}
