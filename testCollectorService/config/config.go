package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds configuration for the test collector service
type Config struct {
	// Kafka configuration
	KafkaBrokers []string
	KafkaTopic   string
	
	// Test configuration
	TokenCount       int           // Number of tokens to simulate
	TradesPerToken   int           // Base number of trades per token per batch
	TradeVariation   int           // Random variation in trades per token
	BatchInterval    time.Duration // Interval between batches
	TestDuration     time.Duration // How long to run the test
	
	// Trade simulation parameters
	MinPrice         float64 // Minimum token price
	MaxPrice         float64 // Maximum token price
	MinAmount        float64 // Minimum trade amount
	MaxAmount        float64 // Maximum trade amount
	BuyProbability   float64 // Probability of buy vs sell (0.0-1.0)
}

// LoadConfig loads configuration from environment variables
func LoadConfig() *Config {
	return &Config{
		KafkaBrokers:     parseStringSlice(getEnv("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopic:       getEnv("KAFKA_TOPIC", "trade-info"),
		TokenCount:       getEnvInt("TOKEN_COUNT", 20),
		TradesPerToken:   getEnvInt("TRADES_PER_TOKEN", 15), // Base 15 trades per token
		TradeVariation:   getEnvInt("TRADE_VARIATION", 10),  // ±10 variation (5-25 trades)
		BatchInterval:    getEnvDuration("BATCH_INTERVAL", 200*time.Millisecond),
		TestDuration:     getEnvDuration("TEST_DURATION", 5*time.Minute),
		MinPrice:         getEnvFloat("MIN_PRICE", 0.001),
		MaxPrice:         getEnvFloat("MAX_PRICE", 10.0),
		MinAmount:        getEnvFloat("MIN_AMOUNT", 10.0),
		MaxAmount:        getEnvFloat("MAX_AMOUNT", 10000.0),
		BuyProbability:   getEnvFloat("BUY_PROBABILITY", 0.5),
	}
}

// Helper functions
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvFloat(key string, defaultValue float64) float64 {
	if value := os.Getenv(key); value != "" {
		if floatValue, err := strconv.ParseFloat(value, 64); err == nil {
			return floatValue
		}
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}

func parseStringSlice(value string) []string {
	if value == "" {
		return []string{}
	}
	
	result := []string{}
	for _, broker := range []string{value} {
		if broker != "" {
			result = append(result, broker)
		}
	}
	return result
}