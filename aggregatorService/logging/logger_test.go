package logging

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"os"
	"strings"
	"testing"
	"time"
)

func TestNewLogger(t *testing.T) {
	logger := NewLogger("test-service", "test-component")
	if logger == nil {
		t.Fatal("NewLogger returned nil")
	}
	
	if logger.service != "test-service" {
		t.Errorf("Expected service 'test-service', got '%s'", logger.service)
	}
	
	if logger.component != "test-component" {
		t.Errorf("Expected component 'test-component', got '%s'", logger.component)
	}
}

func TestLogLevels(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger("test-service", "test-component")
	logger.level = DEBUG // Set to DEBUG level to see all messages
	logger.output = log.New(&buf, "", 0)
	
	// Test different log levels
	logger.Debug("debug message")
	logger.Info("info message")
	logger.Warn("warn message")
	logger.Error("error message")
	
	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	
	// Should have 4 lines (debug, info, warn, error)
	if len(lines) != 4 {
		t.Errorf("Expected 4 log lines, got %d", len(lines))
	}
	
	// Check that each line contains the expected message
	expectedMessages := []string{"debug message", "info message", "warn message", "error message"}
	for i, line := range lines {
		if !strings.Contains(line, expectedMessages[i]) {
			t.Errorf("Line %d should contain '%s', got: %s", i, expectedMessages[i], line)
		}
	}
}

func TestLogWithContext(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger("test-service", "test-component")
	logger.output = log.New(&buf, "", 0)
	
	// Test with context
	contextLogger := logger.WithContext(map[string]interface{}{
		"key1": "value1",
		"key2": 42,
	})
	
	contextLogger.Info("test message")
	
	output := buf.String()
	
	// Parse JSON to verify context fields
	var entry LogEntry
	if err := json.Unmarshal([]byte(output), &entry); err != nil {
		t.Fatalf("Failed to parse log entry: %v", err)
	}
	
	if entry.Fields["key1"] != "value1" {
		t.Errorf("Expected key1='value1', got %v", entry.Fields["key1"])
	}
	
	if entry.Fields["key2"] != float64(42) { // JSON unmarshals numbers as float64
		t.Errorf("Expected key2=42, got %v", entry.Fields["key2"])
	}
}

func TestLogWithTraceID(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger("test-service", "test-component")
	logger.output = log.New(&buf, "", 0)
	
	traceID := "test-trace-123"
	logger.WithTraceID(traceID).Info("test message")
	
	output := buf.String()
	
	var entry LogEntry
	if err := json.Unmarshal([]byte(output), &entry); err != nil {
		t.Fatalf("Failed to parse log entry: %v", err)
	}
	
	if entry.TraceID != traceID {
		t.Errorf("Expected trace_id='%s', got '%s'", traceID, entry.TraceID)
	}
}

func TestLogWithToken(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger("test-service", "test-component")
	logger.output = log.New(&buf, "", 0)
	
	tokenAddress := "0x123456789"
	logger.WithToken(tokenAddress).Info("test message")
	
	output := buf.String()
	
	var entry LogEntry
	if err := json.Unmarshal([]byte(output), &entry); err != nil {
		t.Fatalf("Failed to parse log entry: %v", err)
	}
	
	if entry.TokenAddress != tokenAddress {
		t.Errorf("Expected token_address='%s', got '%s'", tokenAddress, entry.TokenAddress)
	}
}

func TestLogWithError(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger("test-service", "test-component")
	logger.output = log.New(&buf, "", 0)
	
	testErr := errors.New("test error")
	logger.WithError(testErr).Error("error occurred")
	
	output := buf.String()
	
	var entry LogEntry
	if err := json.Unmarshal([]byte(output), &entry); err != nil {
		t.Fatalf("Failed to parse log entry: %v", err)
	}
	
	if entry.Error != testErr.Error() {
		t.Errorf("Expected error='%s', got '%s'", testErr.Error(), entry.Error)
	}
}

func TestTradeProcessed(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger("test-service", "test-component")
	logger.output = log.New(&buf, "", 0)
	
	tokenAddress := "0x123456789"
	traceID := "trace-123"
	duration := 10 * time.Millisecond
	
	// Test successful trade processing
	logger.TradeProcessed(tokenAddress, traceID, duration, true)
	
	output := buf.String()
	
	var entry LogEntry
	if err := json.Unmarshal([]byte(output), &entry); err != nil {
		t.Fatalf("Failed to parse log entry: %v", err)
	}
	
	if entry.Level != "INFO" {
		t.Errorf("Expected level INFO, got %s", entry.Level)
	}
	
	if entry.TokenAddress != tokenAddress {
		t.Errorf("Expected token_address='%s', got '%s'", tokenAddress, entry.TokenAddress)
	}
	
	if entry.TraceID != traceID {
		t.Errorf("Expected trace_id='%s', got '%s'", traceID, entry.TraceID)
	}
	
	if entry.Fields["operation"] != "trade_processing" {
		t.Errorf("Expected operation='trade_processing', got %v", entry.Fields["operation"])
	}
	
	if entry.Fields["success"] != true {
		t.Errorf("Expected success=true, got %v", entry.Fields["success"])
	}
}

func TestAggregationCalculated(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger("test-service", "test-component")
	logger.output = log.New(&buf, "", 0)
	
	tokenAddress := "0x123456789"
	timeframe := "1min"
	traceID := "trace-123"
	duration := 5 * time.Millisecond
	
	logger.AggregationCalculated(tokenAddress, timeframe, traceID, duration, true)
	
	output := buf.String()
	
	var entry LogEntry
	if err := json.Unmarshal([]byte(output), &entry); err != nil {
		t.Fatalf("Failed to parse log entry: %v", err)
	}
	
	if entry.Fields["operation"] != "aggregation_calculation" {
		t.Errorf("Expected operation='aggregation_calculation', got %v", entry.Fields["operation"])
	}
	
	if entry.Fields["timeframe"] != timeframe {
		t.Errorf("Expected timeframe='%s', got %v", timeframe, entry.Fields["timeframe"])
	}
}

func TestRedisOperation(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger("test-service", "test-component")
	logger.output = log.New(&buf, "", 0)
	
	operation := "GET"
	traceID := "trace-123"
	duration := 2 * time.Millisecond
	testErr := errors.New("connection failed")
	
	// Test failed Redis operation
	logger.RedisOperation(operation, traceID, duration, false, testErr)
	
	output := buf.String()
	
	var entry LogEntry
	if err := json.Unmarshal([]byte(output), &entry); err != nil {
		t.Fatalf("Failed to parse log entry: %v", err)
	}
	
	if entry.Level != "ERROR" {
		t.Errorf("Expected level ERROR, got %s", entry.Level)
	}
	
	if entry.Fields["operation"] != "redis_get" {
		t.Errorf("Expected operation='redis_get', got %v", entry.Fields["operation"])
	}
	
	if entry.Fields["success"] != false {
		t.Errorf("Expected success=false, got %v", entry.Fields["success"])
	}
	
	if entry.Error != testErr.Error() {
		t.Errorf("Expected error='%s', got %v", testErr.Error(), entry.Error)
	}
}

func TestLogLevelFromEnv(t *testing.T) {
	// Save original value
	originalLevel := os.Getenv("LOG_LEVEL")
	defer os.Setenv("LOG_LEVEL", originalLevel)
	
	// Test different log levels
	testCases := []struct {
		envValue string
		expected LogLevel
	}{
		{"DEBUG", DEBUG},
		{"INFO", INFO},
		{"WARN", WARN},
		{"ERROR", ERROR},
		{"FATAL", FATAL},
		{"invalid", INFO}, // Should default to INFO
		{"", INFO},        // Should default to INFO
	}
	
	for _, tc := range testCases {
		os.Setenv("LOG_LEVEL", tc.envValue)
		level := getLogLevelFromEnv()
		if level != tc.expected {
			t.Errorf("For LOG_LEVEL='%s', expected %v, got %v", tc.envValue, tc.expected, level)
		}
	}
}

func TestPanicRecovery(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger("test-service", "test-component")
	logger.output = log.New(&buf, "", 0)
	
	tokenAddress := "0x123456789"
	component := "token-processor"
	panicValue := "runtime error: index out of range"
	stackTrace := "goroutine 1 [running]:\ntest.go:123"
	
	logger.PanicRecovery(tokenAddress, component, panicValue, stackTrace)
	
	output := buf.String()
	
	var entry LogEntry
	if err := json.Unmarshal([]byte(output), &entry); err != nil {
		t.Fatalf("Failed to parse log entry: %v", err)
	}
	
	if entry.Level != "ERROR" {
		t.Errorf("Expected level ERROR, got %s", entry.Level)
	}
	
	if entry.TokenAddress != tokenAddress {
		t.Errorf("Expected token_address='%s', got '%s'", tokenAddress, entry.TokenAddress)
	}
	
	if entry.Fields["operation"] != "panic_recovery" {
		t.Errorf("Expected operation='panic_recovery', got %v", entry.Fields["operation"])
	}
	
	if entry.Fields["component"] != component {
		t.Errorf("Expected component='%s', got %v", component, entry.Fields["component"])
	}
	
	if entry.Fields["panic_value"] != panicValue {
		t.Errorf("Expected panic_value='%s', got %v", panicValue, entry.Fields["panic_value"])
	}
}