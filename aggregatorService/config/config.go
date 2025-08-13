package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Config holds all configuration for the aggregator service
type Config struct {
	// Kafka configuration
	KafkaBrokers  []string `json:"kafka_brokers"`
	KafkaTopic    string   `json:"kafka_topic"`
	ConsumerGroup string   `json:"consumer_group"`

	// Redis configuration
	RedisAddr     string `json:"redis_addr"`
	RedisPassword string `json:"redis_password"`
	RedisDB       int    `json:"redis_db"`
	RedisPoolSize int    `json:"redis_pool_size"`

	// Time window configuration
	TimeWindows []TimeWindow `json:"time_windows"`

	// Maintenance configuration
	MaintenanceInterval time.Duration `json:"maintenance_interval"`
	StaleThreshold      time.Duration `json:"stale_threshold"`

	// Worker pool configuration
	MaxWorkers         int `json:"max_workers"`
	MaxTokenProcessors int `json:"max_token_processors"`

	// Metrics configuration
	MetricsEnabled bool `json:"metrics_enabled"`
	MetricsPort    int  `json:"metrics_port"`

	// Logging configuration
	LogLevel string `json:"log_level"`

	// Performance tuning configuration
	RedisMaxRetries   int           `json:"redis_max_retries"`
	RedisRetryDelay   time.Duration `json:"redis_retry_delay"`
	RedisPipelineSize int           `json:"redis_pipeline_size"`
	RedisReadTimeout  time.Duration `json:"redis_read_timeout"`
	RedisWriteTimeout time.Duration `json:"redis_write_timeout"`
	RedisIdleTimeout  time.Duration `json:"redis_idle_timeout"`
	RedisMaxConnAge   time.Duration `json:"redis_max_conn_age"`

	// Memory optimization
	GCTargetPercentage int `json:"gc_target_percentage"`
	MemoryLimitMB      int `json:"memory_limit_mb"`
	TradeBufferSize    int `json:"trade_buffer_size"`

	// Concurrency tuning
	ProcessorChannelSize int `json:"processor_channel_size"`
	WorkerQueueSize      int `json:"worker_queue_size"`
	BatchProcessingSize  int `json:"batch_processing_size"`

	// Internal fields for runtime updates
	mu sync.RWMutex `json:"-"`
}

// TimeWindow represents a time window for aggregation
type TimeWindow struct {
	Duration time.Duration `json:"duration"`
	Name     string        `json:"name"`
}

// ValidationError represents a configuration validation error
type ValidationError struct {
	Field   string
	Value   interface{}
	Message string
}

// Error implements the error interface
func (ve *ValidationError) Error() string {
	return fmt.Sprintf("validation error for field '%s' (value: %v): %s", ve.Field, ve.Value, ve.Message)
}

// ConfigManager manages configuration loading, validation, and runtime updates
type ConfigManager struct {
	config *Config
	mu     sync.RWMutex
}

// NewConfigManager creates a new configuration manager
func NewConfigManager() *ConfigManager {
	return &ConfigManager{}
}

// LoadConfig loads and validates configuration from environment variables
func (cm *ConfigManager) LoadConfig() (*Config, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	config := &Config{
		// Default Kafka settings
		KafkaBrokers:  parseStringSlice(getEnv("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopic:    getEnv("KAFKA_TOPIC", "trade-info"),
		ConsumerGroup: getEnv("KAFKA_CONSUMER_GROUP", "aggregator-service"),

		// Default Redis settings
		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),
		RedisDB:       getEnvInt("REDIS_DB", 0),
		RedisPoolSize: getEnvInt("REDIS_POOL_SIZE", 10),

		// Default time windows (1min, 5min, 15min, 30min, 1hour)
		TimeWindows: parseTimeWindows(getEnv("TIME_WINDOWS", "1min,5min,15min,30min,1hour")),

		// Default maintenance settings
		MaintenanceInterval: getEnvDuration("MAINTENANCE_INTERVAL", 60*time.Second),
		StaleThreshold:      getEnvDuration("STALE_THRESHOLD", 10*time.Second),

		// Default worker pool settings
		MaxWorkers:         getEnvInt("MAX_WORKERS", 100),
		MaxTokenProcessors: getEnvInt("MAX_TOKEN_PROCESSORS", 1000),

		// Default metrics settings
		MetricsEnabled: getEnvBool("METRICS_ENABLED", true),
		MetricsPort:    getEnvInt("METRICS_PORT", 8081), // Changed from 8080 to 8081

		// Default logging settings
		LogLevel: getEnv("LOG_LEVEL", "INFO"),

		// Default performance tuning settings
		RedisMaxRetries:   getEnvInt("REDIS_MAX_RETRIES", 3),
		RedisRetryDelay:   getEnvDuration("REDIS_RETRY_DELAY", 100*time.Millisecond),
		RedisPipelineSize: getEnvInt("REDIS_PIPELINE_SIZE", 100),
		RedisReadTimeout:  getEnvDuration("REDIS_READ_TIMEOUT", 3*time.Second),
		RedisWriteTimeout: getEnvDuration("REDIS_WRITE_TIMEOUT", 3*time.Second),
		RedisIdleTimeout:  getEnvDuration("REDIS_IDLE_TIMEOUT", 5*time.Minute),
		RedisMaxConnAge:   getEnvDuration("REDIS_MAX_CONN_AGE", 30*time.Minute),

		// Default memory optimization settings
		GCTargetPercentage: getEnvInt("GC_TARGET_PERCENTAGE", 100),
		MemoryLimitMB:      getEnvInt("MEMORY_LIMIT_MB", 512),
		TradeBufferSize:    getEnvInt("TRADE_BUFFER_SIZE", 1000),

		// Default concurrency tuning settings
		ProcessorChannelSize: getEnvInt("PROCESSOR_CHANNEL_SIZE", 1000),
		WorkerQueueSize:      getEnvInt("WORKER_QUEUE_SIZE", 10000),
		BatchProcessingSize:  getEnvInt("BATCH_PROCESSING_SIZE", 100),
	}

	// Validate configuration
	if err := cm.validateConfig(config); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	cm.config = config
	return config, nil
}

// GetConfig returns the current configuration (thread-safe)
func (cm *ConfigManager) GetConfig() *Config {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if cm.config == nil {
		return nil
	}

	// Return a copy to prevent external modifications
	configCopy := *cm.config
	configCopy.TimeWindows = make([]TimeWindow, len(cm.config.TimeWindows))
	copy(configCopy.TimeWindows, cm.config.TimeWindows)
	configCopy.KafkaBrokers = make([]string, len(cm.config.KafkaBrokers))
	copy(configCopy.KafkaBrokers, cm.config.KafkaBrokers)

	return &configCopy
}

// UpdateConfig updates specific configuration fields at runtime
func (cm *ConfigManager) UpdateConfig(updates map[string]interface{}) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.config == nil {
		return errors.New("configuration not loaded")
	}

	// Create a copy for validation
	configCopy := *cm.config

	// Apply updates
	for field, value := range updates {
		if err := cm.updateField(&configCopy, field, value); err != nil {
			return fmt.Errorf("failed to update field '%s': %w", field, err)
		}
	}

	// Validate the updated configuration
	if err := cm.validateConfig(&configCopy); err != nil {
		return fmt.Errorf("updated configuration validation failed: %w", err)
	}

	// Apply the updates to the actual config
	*cm.config = configCopy
	return nil
}

// validateConfig validates the configuration
func (cm *ConfigManager) validateConfig(config *Config) error {
	var errors []error

	// Validate Kafka configuration
	if len(config.KafkaBrokers) == 0 {
		errors = append(errors, &ValidationError{
			Field:   "KafkaBrokers",
			Value:   config.KafkaBrokers,
			Message: "at least one Kafka broker must be specified",
		})
	}

	for i, broker := range config.KafkaBrokers {
		if broker == "" {
			errors = append(errors, &ValidationError{
				Field:   fmt.Sprintf("KafkaBrokers[%d]", i),
				Value:   broker,
				Message: "broker address cannot be empty",
			})
		}
	}

	if config.KafkaTopic == "" {
		errors = append(errors, &ValidationError{
			Field:   "KafkaTopic",
			Value:   config.KafkaTopic,
			Message: "Kafka topic cannot be empty",
		})
	}

	if config.ConsumerGroup == "" {
		errors = append(errors, &ValidationError{
			Field:   "ConsumerGroup",
			Value:   config.ConsumerGroup,
			Message: "consumer group cannot be empty",
		})
	}

	// Validate Redis configuration
	if config.RedisAddr == "" {
		errors = append(errors, &ValidationError{
			Field:   "RedisAddr",
			Value:   config.RedisAddr,
			Message: "Redis address cannot be empty",
		})
	}

	if config.RedisDB < 0 || config.RedisDB > 15 {
		errors = append(errors, &ValidationError{
			Field:   "RedisDB",
			Value:   config.RedisDB,
			Message: "Redis DB must be between 0 and 15",
		})
	}

	if config.RedisPoolSize <= 0 {
		errors = append(errors, &ValidationError{
			Field:   "RedisPoolSize",
			Value:   config.RedisPoolSize,
			Message: "Redis pool size must be positive",
		})
	}

	// Validate time windows
	if len(config.TimeWindows) == 0 {
		errors = append(errors, &ValidationError{
			Field:   "TimeWindows",
			Value:   config.TimeWindows,
			Message: "at least one time window must be specified",
		})
	}

	windowNames := make(map[string]bool)
	for i, window := range config.TimeWindows {
		if window.Name == "" {
			errors = append(errors, &ValidationError{
				Field:   fmt.Sprintf("TimeWindows[%d].Name", i),
				Value:   window.Name,
				Message: "time window name cannot be empty",
			})
		}

		if windowNames[window.Name] {
			errors = append(errors, &ValidationError{
				Field:   fmt.Sprintf("TimeWindows[%d].Name", i),
				Value:   window.Name,
				Message: "duplicate time window name",
			})
		}
		windowNames[window.Name] = true

		if window.Duration <= 0 {
			errors = append(errors, &ValidationError{
				Field:   fmt.Sprintf("TimeWindows[%d].Duration", i),
				Value:   window.Duration,
				Message: "time window duration must be positive",
			})
		}

		if window.Duration > 24*time.Hour {
			errors = append(errors, &ValidationError{
				Field:   fmt.Sprintf("TimeWindows[%d].Duration", i),
				Value:   window.Duration,
				Message: "time window duration cannot exceed 24 hours",
			})
		}
	}

	// Validate maintenance configuration
	if config.MaintenanceInterval <= 0 {
		errors = append(errors, &ValidationError{
			Field:   "MaintenanceInterval",
			Value:   config.MaintenanceInterval,
			Message: "maintenance interval must be positive",
		})
	}

	if config.StaleThreshold <= 0 {
		errors = append(errors, &ValidationError{
			Field:   "StaleThreshold",
			Value:   config.StaleThreshold,
			Message: "stale threshold must be positive",
		})
	}

	// Validate worker pool configuration
	if config.MaxWorkers <= 0 {
		errors = append(errors, &ValidationError{
			Field:   "MaxWorkers",
			Value:   config.MaxWorkers,
			Message: "max workers must be positive",
		})
	}

	if config.MaxTokenProcessors <= 0 {
		errors = append(errors, &ValidationError{
			Field:   "MaxTokenProcessors",
			Value:   config.MaxTokenProcessors,
			Message: "max token processors must be positive",
		})
	}

	// Validate metrics configuration
	if config.MetricsPort <= 0 || config.MetricsPort > 65535 {
		errors = append(errors, &ValidationError{
			Field:   "MetricsPort",
			Value:   config.MetricsPort,
			Message: "metrics port must be between 1 and 65535",
		})
	}

	// Validate log level
	validLogLevels := map[string]bool{
		"DEBUG": true, "INFO": true, "WARN": true, "WARNING": true, "ERROR": true, "FATAL": true,
	}
	if !validLogLevels[strings.ToUpper(config.LogLevel)] {
		errors = append(errors, &ValidationError{
			Field:   "LogLevel",
			Value:   config.LogLevel,
			Message: "log level must be one of: DEBUG, INFO, WARN, ERROR, FATAL",
		})
	}

	// Validate performance tuning configuration
	if config.RedisMaxRetries < 0 {
		errors = append(errors, &ValidationError{
			Field:   "RedisMaxRetries",
			Value:   config.RedisMaxRetries,
			Message: "Redis max retries must be non-negative",
		})
	}

	if config.RedisRetryDelay <= 0 {
		errors = append(errors, &ValidationError{
			Field:   "RedisRetryDelay",
			Value:   config.RedisRetryDelay,
			Message: "Redis retry delay must be positive",
		})
	}

	if config.RedisPipelineSize <= 0 {
		errors = append(errors, &ValidationError{
			Field:   "RedisPipelineSize",
			Value:   config.RedisPipelineSize,
			Message: "Redis pipeline size must be positive",
		})
	}

	if config.GCTargetPercentage <= 0 || config.GCTargetPercentage > 1000 {
		errors = append(errors, &ValidationError{
			Field:   "GCTargetPercentage",
			Value:   config.GCTargetPercentage,
			Message: "GC target percentage must be between 1 and 1000",
		})
	}

	if config.MemoryLimitMB <= 0 {
		errors = append(errors, &ValidationError{
			Field:   "MemoryLimitMB",
			Value:   config.MemoryLimitMB,
			Message: "Memory limit must be positive",
		})
	}

	if config.TradeBufferSize <= 0 {
		errors = append(errors, &ValidationError{
			Field:   "TradeBufferSize",
			Value:   config.TradeBufferSize,
			Message: "Trade buffer size must be positive",
		})
	}

	if config.ProcessorChannelSize <= 0 {
		errors = append(errors, &ValidationError{
			Field:   "ProcessorChannelSize",
			Value:   config.ProcessorChannelSize,
			Message: "Processor channel size must be positive",
		})
	}

	if config.WorkerQueueSize <= 0 {
		errors = append(errors, &ValidationError{
			Field:   "WorkerQueueSize",
			Value:   config.WorkerQueueSize,
			Message: "Worker queue size must be positive",
		})
	}

	if config.BatchProcessingSize <= 0 {
		errors = append(errors, &ValidationError{
			Field:   "BatchProcessingSize",
			Value:   config.BatchProcessingSize,
			Message: "Batch processing size must be positive",
		})
	}

	if len(errors) > 0 {
		return &MultiValidationError{Errors: errors}
	}

	return nil
}

// updateField updates a specific configuration field
func (cm *ConfigManager) updateField(config *Config, field string, value interface{}) error {
	switch field {
	case "MaintenanceInterval":
		if duration, ok := value.(time.Duration); ok {
			config.MaintenanceInterval = duration
		} else if str, ok := value.(string); ok {
			if duration, err := time.ParseDuration(str); err == nil {
				config.MaintenanceInterval = duration
			} else {
				return fmt.Errorf("invalid duration format: %s", str)
			}
		} else {
			return fmt.Errorf("invalid type for MaintenanceInterval: %T", value)
		}

	case "StaleThreshold":
		if duration, ok := value.(time.Duration); ok {
			config.StaleThreshold = duration
		} else if str, ok := value.(string); ok {
			if duration, err := time.ParseDuration(str); err == nil {
				config.StaleThreshold = duration
			} else {
				return fmt.Errorf("invalid duration format: %s", str)
			}
		} else {
			return fmt.Errorf("invalid type for StaleThreshold: %T", value)
		}

	case "MaxWorkers":
		if intVal, ok := value.(int); ok {
			config.MaxWorkers = intVal
		} else {
			return fmt.Errorf("invalid type for MaxWorkers: %T", value)
		}

	case "MaxTokenProcessors":
		if intVal, ok := value.(int); ok {
			config.MaxTokenProcessors = intVal
		} else {
			return fmt.Errorf("invalid type for MaxTokenProcessors: %T", value)
		}

	case "LogLevel":
		if str, ok := value.(string); ok {
			config.LogLevel = str
		} else {
			return fmt.Errorf("invalid type for LogLevel: %T", value)
		}

	case "MetricsEnabled":
		if boolVal, ok := value.(bool); ok {
			config.MetricsEnabled = boolVal
		} else {
			return fmt.Errorf("invalid type for MetricsEnabled: %T", value)
		}

	default:
		return fmt.Errorf("field '%s' is not updatable at runtime", field)
	}

	return nil
}

// MultiValidationError represents multiple validation errors
type MultiValidationError struct {
	Errors []error
}

// Error implements the error interface
func (mve *MultiValidationError) Error() string {
	var messages []string
	for _, err := range mve.Errors {
		messages = append(messages, err.Error())
	}
	return fmt.Sprintf("multiple validation errors: %s", strings.Join(messages, "; "))
}

// LoadConfig is a convenience function for backward compatibility
func LoadConfig() *Config {
	cm := NewConfigManager()
	config, err := cm.LoadConfig()
	if err != nil {
		// For backward compatibility, panic on validation errors
		panic(fmt.Sprintf("Failed to load configuration: %v", err))
	}
	return config
}

// Helper functions

// getEnv gets environment variable with default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvInt gets environment variable as integer with default value
func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

// getEnvBool gets environment variable as boolean with default value
func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}

// getEnvDuration gets environment variable as duration with default value
func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}

// parseStringSlice parses a comma-separated string into a slice
func parseStringSlice(value string) []string {
	if value == "" {
		return []string{}
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// parseTimeWindows parses time window configuration
func parseTimeWindows(value string) []TimeWindow {
	// Return defaults for empty input
	if value == "" {
		return []TimeWindow{
			{Duration: 1 * time.Minute, Name: "1min"},
			{Duration: 5 * time.Minute, Name: "5min"},
			{Duration: 15 * time.Minute, Name: "15min"},
			{Duration: 30 * time.Minute, Name: "30min"},
			{Duration: 1 * time.Hour, Name: "1hour"},
		}
	}

	parts := strings.Split(value, ",")
	windows := make([]TimeWindow, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Parse duration from name (e.g., "1min" -> 1 minute)
		var duration time.Duration
		var err error

		switch part {
		case "1min":
			duration = 1 * time.Minute
		case "5min":
			duration = 5 * time.Minute
		case "15min":
			duration = 15 * time.Minute
		case "30min":
			duration = 30 * time.Minute
		case "1hour":
			duration = 1 * time.Hour
		default:
			// Try to parse as duration
			duration, err = time.ParseDuration(part)
			if err != nil {
				continue // Skip invalid durations
			}
		}

		windows = append(windows, TimeWindow{
			Duration: duration,
			Name:     part,
		})
	}

	// If no valid windows were parsed, return defaults
	if len(windows) == 0 {
		return []TimeWindow{
			{Duration: 1 * time.Minute, Name: "1min"},
			{Duration: 5 * time.Minute, Name: "5min"},
			{Duration: 15 * time.Minute, Name: "15min"},
			{Duration: 30 * time.Minute, Name: "30min"},
			{Duration: 1 * time.Hour, Name: "1hour"},
		}
	}

	return windows
}
