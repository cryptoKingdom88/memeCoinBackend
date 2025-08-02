package config

import (
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// Config holds all configuration values for the application
type Config struct {
	BitqueryAPIKey string
	KafkaBrokers   string
	
	// Database configuration
	DBHost     string
	DBPort     string
	DBUser     string
	DBPassword string
	DBName     string
	
	// Batch processing interval in seconds
	BatchInterval int
}

// LoadConfig loads configuration from environment variables
func LoadConfig() Config {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Can't load .env file: %v", err)
	}

	// Parse batch interval with default value of 30 seconds
	batchInterval := 30
	if intervalStr := os.Getenv("BATCH_INTERVAL"); intervalStr != "" {
		if parsed, err := strconv.Atoi(intervalStr); err == nil {
			batchInterval = parsed
		}
	}

	return Config{
		BitqueryAPIKey: os.Getenv("BITQUERY_API_KEY"),
		KafkaBrokers:   os.Getenv("KAFKA_BROKERS"),
		
		DBHost:     os.Getenv("DB_HOST"),
		DBPort:     os.Getenv("DB_PORT"),
		DBUser:     os.Getenv("DB_USER"),
		DBPassword: os.Getenv("DB_PASSWORD"),
		DBName:     os.Getenv("DB_NAME"),
		
		BatchInterval: batchInterval,
	}
}
