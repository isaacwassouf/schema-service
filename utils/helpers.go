package utils

import (
	"database/sql"
	"fmt"
	"github.com/isaacwassouf/schema-service/shared"
	"github.com/joho/godotenv"
	"os"
	"strings"

	pb "github.com/isaacwassouf/schema-service/protobufs/schema_management_service"
)

func LoadEnvVarsFromFile() error {
	err := godotenv.Load()
	if err != nil {
		return err
	}
	return nil
}

func GetEnvVar(key string, defaultValue string) string {
	value, exists := os.LookupEnv(key)
	if !exists {
		return defaultValue
	}
	return value
}

func ReadTemplateFile(filepath string) (string, error) {
	templateFileBytes, err := os.ReadFile(filepath)
	if err != nil {
		return "", err
	}
	return string(templateFileBytes), nil
}

func CheckTableExists(db *sql.DB, tableName string) (bool, error) {
	query := fmt.Sprintf("SHOW TABLES LIKE '%s'", tableName)
	rows, err := db.Query(query)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	return rows.Next(), nil
}

func CheckColumnExists(db *sql.DB, tableName string, columnName string) (bool, error) {
	query := fmt.Sprintf("SHOW COLUMNS FROM %s LIKE '%s'", tableName, columnName)
	rows, err := db.Query(query)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	return rows.Next(), nil
}

func GetIntColumnType(column *pb.Column) (string, error) {
	var columnType string
	switch column.GetIntColumn().GetType() {
	case pb.IntegerColumnType_INT:
		columnType = "INT"
	case pb.IntegerColumnType_BIGINT:
		columnType = "BIGINT"
	case pb.IntegerColumnType_SMALLINT:
		columnType = "SMALLINT"
	case pb.IntegerColumnType_TINYINT:
		columnType = "TINYINT"
	case pb.IntegerColumnType_MEDIUMINT:
		columnType = "MEDIUMINT"
	default:
		return "", fmt.Errorf("invalid integer column type")
	}

	// check if the int column is unsigned
	if column.GetIntColumn().GetIsUnsigned() {
		columnType += " UNSIGNED"
	}

	// check if the int column is auto increment
	if column.GetIntColumn().GetAutoIncrement() {
		columnType += " AUTO_INCREMENT"
	}

	return columnType, nil
}

func GetVarCharColumnType(column *pb.Column) (string, error) {
	// check if the length is provided
	if column.GetVarcharColumn().Length == 0 {
		return "", fmt.Errorf("varchar length is required")
	}

	// check if the length is between 1 and 65535
	if column.GetVarcharColumn().Length < 1 || column.GetVarcharColumn().Length > 65535 {
		return "", fmt.Errorf("varchar length must be between 1 and 65535")
	}

	return fmt.Sprintf("VARCHAR(%d)", column.GetVarcharColumn().Length), nil
}

func GetColumnFromType(columnDetails *shared.RawColumnDetails) (*pb.Column, error) {
	column := &pb.Column{}
	// the column type is an int
	if columnDetails.DataType == "int" {
		column.Type = &pb.Column_IntColumn{
			IntColumn: &pb.IntegerColumn{
				Type: pb.IntegerColumnType_INT,
			},
		}

		// check if its unsigned
		if strings.Contains(columnDetails.ColumnType, "unsigned") {
			column.GetIntColumn().IsUnsigned = true
		}

		// check if its auto increment
		if columnDetails.Extra == "auto_increment" {
			column.GetIntColumn().AutoIncrement = true
		}

		return column, nil
	}

	// the column is of type bigint
	if columnDetails.DataType == "bigint" {
		column.Type = &pb.Column_IntColumn{
			IntColumn: &pb.IntegerColumn{
				Type: pb.IntegerColumnType_BIGINT,
			},
		}

		// check if its unsigned
		if strings.Contains(columnDetails.ColumnType, "unsigned") {
			column.GetIntColumn().IsUnsigned = true
		}

		// check if its auto increment
		if columnDetails.Extra == "auto_increment" {
			column.GetIntColumn().AutoIncrement = true
		}

		return column, nil
	}

	// the column type is a SMALLINT
	if columnDetails.DataType == "smallint" {
		column.Type = &pb.Column_IntColumn{
			IntColumn: &pb.IntegerColumn{
				Type: pb.IntegerColumnType_SMALLINT,
			},
		}

		// check if its unsigned
		if strings.Contains(columnDetails.ColumnType, "unsigned") {
			column.GetIntColumn().IsUnsigned = true
		}

		// check if its auto increment
		if columnDetails.Extra == "auto_increment" {
			column.GetIntColumn().AutoIncrement = true
		}

		return column, nil
	}

	// the column type is a mediumint
	if columnDetails.DataType == "mediumint" {
		column.Type = &pb.Column_IntColumn{
			IntColumn: &pb.IntegerColumn{
				Type: pb.IntegerColumnType_MEDIUMINT,
			},
		}

		// check if its unsigned
		if strings.Contains(columnDetails.ColumnType, "unsigned") {
			column.GetIntColumn().IsUnsigned = true
		}

		// check if its auto increment
		if columnDetails.Extra == "auto_increment" {
			column.GetIntColumn().AutoIncrement = true
		}

		return column, nil
	}

	// the column type is a tinyint, aka a boolean
	if columnDetails.DataType == "tinyint" {
		column.Type = &pb.Column_BoolColumn{}

		return column, nil
	}

	// the column type is a varchar
	if columnDetails.DataType == "varchar" {
		column.Type = &pb.Column_VarcharColumn{
			VarcharColumn: &pb.VarCharColumn{
				Length: 122,
			},
		}

		return column, nil
	}

	// the column type is a timestamp
	if columnDetails.DataType == "timestamp" {
		column.Type = &pb.Column_TimestampColumn{}

		return column, nil
	}

	// return an error if the column type is not supported
	return nil, fmt.Errorf("unsupported column type")
}
