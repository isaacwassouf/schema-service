package utils

import (
	"database/sql"
	"fmt"
	"github.com/joho/godotenv"
	"os"

	pb "github.com/isaacwassouf/schema-service/protobufs/schema_management_service"
)

func LoadEnvVarsFromFile() error {
	err := godotenv.Load()
	if err != nil {
		return err
	}
	return nil
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
