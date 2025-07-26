package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	BitqueryAPIKey string
	KafkaBrokers   string
}

func LoadConfig() Config {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Can't load .env file: %v", err)
	}

	return Config{
		BitqueryAPIKey: os.Getenv("BITQUERY_API_KEY"),
		KafkaBrokers:   os.Getenv("KAFKA_BROKERS"),
	}
}
