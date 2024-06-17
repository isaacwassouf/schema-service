package utils

import (
	"github.com/joho/godotenv"
)

func LoadEnvVarsFromFile() error {
	err := godotenv.Load()
	if err != nil {
		return err
	}
	return nil
}
