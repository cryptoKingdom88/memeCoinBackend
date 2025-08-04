package config

import (
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// Config holds all configuration values for the aggregator service
type Config struct {
	KafkaBrokers        string
	AggregationInterval int // Aggregation interval in seconds
}

// LoadConfig loads configuration from environment variables
func LoadConfig() Config {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Can't load .env file: %v", err)
	}

	// Parse aggregation interval with default value of 60 seconds
	aggregationInterval := 60
	if intervalStr := os.Getenv("AGGREGATION_INTERVAL"); intervalStr != "" {
		if parsed, err := strconv.Atoi(intervalStr); err == nil {
			aggregationInterval = parsed
		}
	}

	return Config{
		KafkaBrokers:        os.Getenv("KAFKA_BROKERS"),
		AggregationInterval: aggregationInterval,
	}
}
