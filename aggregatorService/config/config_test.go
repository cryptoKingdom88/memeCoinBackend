package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestNewConfigManager(t *testing.T) {
	cm := NewConfigManager()
	if cm == nil {
		t.Fatal("NewConfigManager returned nil")
	}
}

func TestLoadConfig(t *testing.T) {
	// Save original environment
	originalEnv := make(map[string]string)
	envVars := []string{
		"KAFKA_BROKERS", "KAFKA_TOPIC", "KAFKA_CONSUMER_GROUP",
		"REDIS_ADDR", "REDIS_PASSWORD", "REDIS_DB", "REDIS_POOL_SIZE",
		"TIME_WINDOWS", "MAINTENANCE_INTERVAL", "STALE_THRESHOLD",
		"MAX_WORKERS", "MAX_TOKEN_PROCESSORS", "METRICS_ENABLED", "METRICS_PORT", "LOG_LEVEL",
	}
	
	for _, env := range envVars {
		originalEnv[env] = os.Getenv(env)
		os.Unsetenv(env)
	}
	defer func() {
		for env, value := range originalEnv {
			if value != "" {
				os.Setenv(env, value)
			}
		}
	}()

	cm := NewConfigManager()
	config, err := cm.LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	// Test default values
	if len(config.KafkaBrokers) != 1 || config.KafkaBrokers[0] != "localhost:9092" {
		t.Errorf("Expected default Kafka brokers [localhost:9092], got %v", config.KafkaBrokers)
	}

	if config.KafkaTopic != "trade-info" {
		t.Errorf("Expected default Kafka topic 'trade-info', got '%s'", config.KafkaTopic)
	}

	if config.ConsumerGroup != "aggregator-service" {
		t.Errorf("Expected default consumer group 'aggregator-service', got '%s'", config.ConsumerGroup)
	}

	if config.RedisAddr != "localhost:6379" {
		t.Errorf("Expected default Redis address 'localhost:6379', got '%s'", config.RedisAddr)
	}

	if config.RedisDB != 0 {
		t.Errorf("Expected default Redis DB 0, got %d", config.RedisDB)
	}

	if config.RedisPoolSize != 10 {
		t.Errorf("Expected default Redis pool size 10, got %d", config.RedisPoolSize)
	}

	if len(config.TimeWindows) != 5 {
		t.Errorf("Expected 5 default time windows, got %d", len(config.TimeWindows))
	}

	if config.MaintenanceInterval != 60*time.Second {
		t.Errorf("Expected default maintenance interval 60s, got %v", config.MaintenanceInterval)
	}

	if config.StaleThreshold != 10*time.Second {
		t.Errorf("Expected default stale threshold 10s, got %v", config.StaleThreshold)
	}

	if config.MaxWorkers != 100 {
		t.Errorf("Expected default max workers 100, got %d", config.MaxWorkers)
	}

	if config.MaxTokenProcessors != 1000 {
		t.Errorf("Expected default max token processors 1000, got %d", config.MaxTokenProcessors)
	}

	if !config.MetricsEnabled {
		t.Errorf("Expected default metrics enabled true, got %v", config.MetricsEnabled)
	}

	if config.MetricsPort != 8080 {
		t.Errorf("Expected default metrics port 8080, got %d", config.MetricsPort)
	}

	if config.LogLevel != "INFO" {
		t.Errorf("Expected default log level 'INFO', got '%s'", config.LogLevel)
	}
}

func TestLoadConfigWithEnvironment(t *testing.T) {
	// Set environment variables
	os.Setenv("KAFKA_BROKERS", "broker1:9092,broker2:9092")
	os.Setenv("KAFKA_TOPIC", "custom-topic")
	os.Setenv("REDIS_ADDR", "redis.example.com:6379")
	os.Setenv("REDIS_DB", "5")
	os.Setenv("REDIS_POOL_SIZE", "20")
	os.Setenv("TIME_WINDOWS", "30s,2m,10m")
	os.Setenv("MAINTENANCE_INTERVAL", "30s")
	os.Setenv("STALE_THRESHOLD", "5s")
	os.Setenv("MAX_WORKERS", "200")
	os.Setenv("METRICS_ENABLED", "false")
	os.Setenv("METRICS_PORT", "9090")
	os.Setenv("LOG_LEVEL", "DEBUG")

	defer func() {
		envVars := []string{
			"KAFKA_BROKERS", "KAFKA_TOPIC", "REDIS_ADDR", "REDIS_DB", "REDIS_POOL_SIZE",
			"TIME_WINDOWS", "MAINTENANCE_INTERVAL", "STALE_THRESHOLD", "MAX_WORKERS",
			"METRICS_ENABLED", "METRICS_PORT", "LOG_LEVEL",
		}
		for _, env := range envVars {
			os.Unsetenv(env)
		}
	}()

	cm := NewConfigManager()
	config, err := cm.LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	// Test environment values
	expectedBrokers := []string{"broker1:9092", "broker2:9092"}
	if len(config.KafkaBrokers) != 2 || config.KafkaBrokers[0] != expectedBrokers[0] || config.KafkaBrokers[1] != expectedBrokers[1] {
		t.Errorf("Expected Kafka brokers %v, got %v", expectedBrokers, config.KafkaBrokers)
	}

	if config.KafkaTopic != "custom-topic" {
		t.Errorf("Expected Kafka topic 'custom-topic', got '%s'", config.KafkaTopic)
	}

	if config.RedisAddr != "redis.example.com:6379" {
		t.Errorf("Expected Redis address 'redis.example.com:6379', got '%s'", config.RedisAddr)
	}

	if config.RedisDB != 5 {
		t.Errorf("Expected Redis DB 5, got %d", config.RedisDB)
	}

	if config.RedisPoolSize != 20 {
		t.Errorf("Expected Redis pool size 20, got %d", config.RedisPoolSize)
	}

	if len(config.TimeWindows) != 3 {
		t.Errorf("Expected 3 time windows, got %d", len(config.TimeWindows))
	}

	if config.MaintenanceInterval != 30*time.Second {
		t.Errorf("Expected maintenance interval 30s, got %v", config.MaintenanceInterval)
	}

	if config.StaleThreshold != 5*time.Second {
		t.Errorf("Expected stale threshold 5s, got %v", config.StaleThreshold)
	}

	if config.MaxWorkers != 200 {
		t.Errorf("Expected max workers 200, got %d", config.MaxWorkers)
	}

	if config.MetricsEnabled {
		t.Errorf("Expected metrics enabled false, got %v", config.MetricsEnabled)
	}

	if config.MetricsPort != 9090 {
		t.Errorf("Expected metrics port 9090, got %d", config.MetricsPort)
	}

	if config.LogLevel != "DEBUG" {
		t.Errorf("Expected log level 'DEBUG', got '%s'", config.LogLevel)
	}
}

func TestValidateConfig(t *testing.T) {
	cm := NewConfigManager()

	testCases := []struct {
		name        string
		config      *Config
		expectError bool
		errorField  string
	}{
		{
			name: "Valid configuration",
			config: &Config{
				KafkaBrokers:        []string{"localhost:9092"},
				KafkaTopic:          "test-topic",
				ConsumerGroup:       "test-group",
				RedisAddr:           "localhost:6379",
				RedisDB:             0,
				RedisPoolSize:       10,
				TimeWindows:         []TimeWindow{{Duration: 1 * time.Minute, Name: "1min"}},
				MaintenanceInterval: 60 * time.Second,
				StaleThreshold:      10 * time.Second,
				MaxWorkers:          100,
				MaxTokenProcessors:  1000,
				MetricsEnabled:      true,
				MetricsPort:         8080,
				LogLevel:            "INFO",
			},
			expectError: false,
		},
		{
			name: "Empty Kafka brokers",
			config: &Config{
				KafkaBrokers:        []string{},
				KafkaTopic:          "test-topic",
				ConsumerGroup:       "test-group",
				RedisAddr:           "localhost:6379",
				RedisDB:             0,
				RedisPoolSize:       10,
				TimeWindows:         []TimeWindow{{Duration: 1 * time.Minute, Name: "1min"}},
				MaintenanceInterval: 60 * time.Second,
				StaleThreshold:      10 * time.Second,
				MaxWorkers:          100,
				MaxTokenProcessors:  1000,
				MetricsEnabled:      true,
				MetricsPort:         8080,
				LogLevel:            "INFO",
			},
			expectError: true,
			errorField:  "KafkaBrokers",
		},
		{
			name: "Empty Kafka topic",
			config: &Config{
				KafkaBrokers:        []string{"localhost:9092"},
				KafkaTopic:          "",
				ConsumerGroup:       "test-group",
				RedisAddr:           "localhost:6379",
				RedisDB:             0,
				RedisPoolSize:       10,
				TimeWindows:         []TimeWindow{{Duration: 1 * time.Minute, Name: "1min"}},
				MaintenanceInterval: 60 * time.Second,
				StaleThreshold:      10 * time.Second,
				MaxWorkers:          100,
				MaxTokenProcessors:  1000,
				MetricsEnabled:      true,
				MetricsPort:         8080,
				LogLevel:            "INFO",
			},
			expectError: true,
			errorField:  "KafkaTopic",
		},
		{
			name: "Invalid Redis DB",
			config: &Config{
				KafkaBrokers:        []string{"localhost:9092"},
				KafkaTopic:          "test-topic",
				ConsumerGroup:       "test-group",
				RedisAddr:           "localhost:6379",
				RedisDB:             20, // Invalid: > 15
				RedisPoolSize:       10,
				TimeWindows:         []TimeWindow{{Duration: 1 * time.Minute, Name: "1min"}},
				MaintenanceInterval: 60 * time.Second,
				StaleThreshold:      10 * time.Second,
				MaxWorkers:          100,
				MaxTokenProcessors:  1000,
				MetricsEnabled:      true,
				MetricsPort:         8080,
				LogLevel:            "INFO",
			},
			expectError: true,
			errorField:  "RedisDB",
		},
		{
			name: "Invalid time window duration",
			config: &Config{
				KafkaBrokers:        []string{"localhost:9092"},
				KafkaTopic:          "test-topic",
				ConsumerGroup:       "test-group",
				RedisAddr:           "localhost:6379",
				RedisDB:             0,
				RedisPoolSize:       10,
				TimeWindows:         []TimeWindow{{Duration: -1 * time.Minute, Name: "invalid"}},
				MaintenanceInterval: 60 * time.Second,
				StaleThreshold:      10 * time.Second,
				MaxWorkers:          100,
				MaxTokenProcessors:  1000,
				MetricsEnabled:      true,
				MetricsPort:         8080,
				LogLevel:            "INFO",
			},
			expectError: true,
			errorField:  "TimeWindows[0].Duration",
		},
		{
			name: "Invalid log level",
			config: &Config{
				KafkaBrokers:        []string{"localhost:9092"},
				KafkaTopic:          "test-topic",
				ConsumerGroup:       "test-group",
				RedisAddr:           "localhost:6379",
				RedisDB:             0,
				RedisPoolSize:       10,
				TimeWindows:         []TimeWindow{{Duration: 1 * time.Minute, Name: "1min"}},
				MaintenanceInterval: 60 * time.Second,
				StaleThreshold:      10 * time.Second,
				MaxWorkers:          100,
				MaxTokenProcessors:  1000,
				MetricsEnabled:      true,
				MetricsPort:         8080,
				LogLevel:            "INVALID",
			},
			expectError: true,
			errorField:  "LogLevel",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := cm.validateConfig(tc.config)
			
			if tc.expectError {
				if err == nil {
					t.Errorf("Expected validation error, got nil")
				} else if !strings.Contains(err.Error(), tc.errorField) {
					t.Errorf("Expected error to contain field '%s', got: %v", tc.errorField, err)
				}
			} else {
				if err != nil {
					t.Errorf("Expected no validation error, got: %v", err)
				}
			}
		})
	}
}

func TestUpdateConfig(t *testing.T) {
	cm := NewConfigManager()
	
	// Load initial config
	_, err := cm.LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	// Test valid updates
	updates := map[string]interface{}{
		"MaintenanceInterval": "30s",
		"StaleThreshold":      5 * time.Second,
		"MaxWorkers":          200,
		"LogLevel":            "DEBUG",
		"MetricsEnabled":      false,
	}

	err = cm.UpdateConfig(updates)
	if err != nil {
		t.Fatalf("UpdateConfig failed: %v", err)
	}

	// Verify updates
	config := cm.GetConfig()
	if config.MaintenanceInterval != 30*time.Second {
		t.Errorf("Expected maintenance interval 30s, got %v", config.MaintenanceInterval)
	}

	if config.StaleThreshold != 5*time.Second {
		t.Errorf("Expected stale threshold 5s, got %v", config.StaleThreshold)
	}

	if config.MaxWorkers != 200 {
		t.Errorf("Expected max workers 200, got %d", config.MaxWorkers)
	}

	if config.LogLevel != "DEBUG" {
		t.Errorf("Expected log level 'DEBUG', got '%s'", config.LogLevel)
	}

	if config.MetricsEnabled {
		t.Errorf("Expected metrics enabled false, got %v", config.MetricsEnabled)
	}
}

func TestUpdateConfigInvalidField(t *testing.T) {
	cm := NewConfigManager()
	
	// Load initial config
	_, err := cm.LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	// Test invalid field update
	updates := map[string]interface{}{
		"KafkaTopic": "new-topic", // Not updatable at runtime
	}

	err = cm.UpdateConfig(updates)
	if err == nil {
		t.Error("Expected error for non-updatable field, got nil")
	}

	if !strings.Contains(err.Error(), "not updatable at runtime") {
		t.Errorf("Expected error about non-updatable field, got: %v", err)
	}
}

func TestUpdateConfigInvalidValue(t *testing.T) {
	cm := NewConfigManager()
	
	// Load initial config
	_, err := cm.LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	// Test invalid value update
	updates := map[string]interface{}{
		"MaxWorkers": -10, // Invalid: negative
	}

	err = cm.UpdateConfig(updates)
	if err == nil {
		t.Error("Expected validation error for invalid value, got nil")
	}
}

func TestGetConfig(t *testing.T) {
	cm := NewConfigManager()
	
	// Test before loading config
	config := cm.GetConfig()
	if config != nil {
		t.Error("Expected nil config before loading, got non-nil")
	}

	// Load config
	_, err := cm.LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	// Test after loading config
	config = cm.GetConfig()
	if config == nil {
		t.Fatal("Expected non-nil config after loading, got nil")
	}

	// Test that returned config is a copy (modifications don't affect original)
	originalBrokers := config.KafkaBrokers[0]
	config.KafkaBrokers[0] = "modified"

	config2 := cm.GetConfig()
	if config2.KafkaBrokers[0] != originalBrokers {
		t.Error("GetConfig should return a copy, but original was modified")
	}
}

func TestParseStringSlice(t *testing.T) {
	testCases := []struct {
		input    string
		expected []string
	}{
		{"", []string{}},
		{"single", []string{"single"}},
		{"one,two,three", []string{"one", "two", "three"}},
		{"one, two , three ", []string{"one", "two", "three"}},
		{"one,,three", []string{"one", "three"}},
		{" , , ", []string{}},
	}

	for _, tc := range testCases {
		result := parseStringSlice(tc.input)
		if len(result) != len(tc.expected) {
			t.Errorf("For input '%s', expected length %d, got %d", tc.input, len(tc.expected), len(result))
			continue
		}

		for i, expected := range tc.expected {
			if result[i] != expected {
				t.Errorf("For input '%s', expected[%d]='%s', got '%s'", tc.input, i, expected, result[i])
			}
		}
	}
}

func TestParseTimeWindows(t *testing.T) {
	testCases := []struct {
		input    string
		expected int // number of windows
	}{
		{"", 5},                    // Should return defaults
		{"1min", 1},
		{"1min,5min,15min", 3},
		{"30s,2m,1h", 3},
		{"invalid", 5},             // Should return defaults for invalid input
		{"1min,invalid,5min", 2},   // Should skip invalid entries
	}

	for _, tc := range testCases {
		result := parseTimeWindows(tc.input)
		if len(result) != tc.expected {
			t.Errorf("For input '%s', expected %d windows, got %d", tc.input, tc.expected, len(result))
		}
	}
}

func TestBackwardCompatibility(t *testing.T) {
	// Test that the old LoadConfig function still works
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("LoadConfig panicked: %v", r)
		}
	}()

	config := LoadConfig()
	if config == nil {
		t.Error("LoadConfig returned nil")
	}
}