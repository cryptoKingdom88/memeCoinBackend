package config

import (
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	// Server Configuration
	Port               string
	WSReadBufferSize   int
	WSWriteBufferSize  int

	// Kafka Configuration
	KafkaBrokers      []string
	KafkaConsumerGroup string

	// Performance Configuration
	MaxClients        int
	MessageBufferSize int
	KafkaBatchSize    int

	// Token Management Configuration
	NewTokensCacheSize      int
	TrendingTokensCacheSize int
	TokenCacheTTLMinutes    int

	// Redis Configuration
	RedisAddr     string
	RedisPassword string
	RedisDB       int

	// Database Configuration (fallback)
	DBHost     string
	DBPort     int
	DBName     string
	DBUser     string
	DBPassword string

	// Logging Configuration
	LogLevel  string
	LogFormat string
}

func Load() *Config {
	// Load .env file if it exists
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	return &Config{
		Port:               getEnv("PORT", "8080"),
		WSReadBufferSize:   getEnvAsInt("WS_READ_BUFFER_SIZE", 1024),
		WSWriteBufferSize:  getEnvAsInt("WS_WRITE_BUFFER_SIZE", 1024),
		KafkaBrokers:       getEnvAsSlice("KAFKA_BROKERS", []string{"localhost:9092"}),
		KafkaConsumerGroup: getEnv("KAFKA_CONSUMER_GROUP", "backend-service"),
		MaxClients:         getEnvAsInt("MAX_CLIENTS", 1000),
		MessageBufferSize:  getEnvAsInt("MESSAGE_BUFFER_SIZE", 100),
		KafkaBatchSize:     getEnvAsInt("KAFKA_BATCH_SIZE", 100),
		
		// Token Management Configuration
		NewTokensCacheSize:      getEnvAsInt("NEW_TOKENS_CACHE_SIZE", 30),
		TrendingTokensCacheSize: getEnvAsInt("TRENDING_TOKENS_CACHE_SIZE", 30),
		TokenCacheTTLMinutes:    getEnvAsInt("TOKEN_CACHE_TTL_MINUTES", 10),
		
		// Redis Configuration
		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),
		RedisDB:       getEnvAsInt("REDIS_DB", 0),
		
		// Database Configuration
		DBHost:     getEnv("DB_HOST", "localhost"),
		DBPort:     getEnvAsInt("DB_PORT", 5432),
		DBName:     getEnv("DB_NAME", "memecoin"),
		DBUser:     getEnv("DB_USER", "postgres"),
		DBPassword: getEnv("DB_PASSWORD", "password"),
		
		// Logging Configuration
		LogLevel:  getEnv("LOG_LEVEL", "INFO"),
		LogFormat: getEnv("LOG_FORMAT", "json"),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvAsSlice(key string, defaultValue []string) []string {
	if value := os.Getenv(key); value != "" {
		return strings.Split(value, ",")
	}
	return defaultValue
}