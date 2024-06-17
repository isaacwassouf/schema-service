package utils

import (
	"database/sql"
	"fmt"
	"github.com/joho/godotenv"
	"os"
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
