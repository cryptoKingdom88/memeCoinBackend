package logging

import (
	"errors"
	"testing"
)

func TestCategorizeError(t *testing.T) {
	logger := NewLogger("test-service", "test-component")
	categorizer := NewErrorCategorizer(logger)
	
	testCases := []struct {
		name        string
		err         error
		component   string
		operation   string
		expectedCat ErrorCategory
		expectedSev ErrorSeverity
		expectedRec bool
	}{
		{
			name:        "Redis connection error",
			err:         errors.New("redis: connection refused"),
			component:   "redis-manager",
			operation:   "get",
			expectedCat: ErrorCategoryRedis,
			expectedSev: ErrorSeverityCritical,
			expectedRec: true,
		},
		{
			name:        "Kafka broker error",
			err:         errors.New("kafka: no available brokers"),
			component:   "kafka-consumer",
			operation:   "consume",
			expectedCat: ErrorCategoryKafka,
			expectedSev: ErrorSeverityCritical,
			expectedRec: true,
		},
		{
			name:        "Validation error",
			err:         errors.New("invalid trade data format"),
			component:   "trade-processor",
			operation:   "validate",
			expectedCat: ErrorCategoryValidation,
			expectedSev: ErrorSeverityLow,
			expectedRec: false,
		},
		{
			name:        "Memory error",
			err:         errors.New("out of memory"),
			component:   "aggregator",
			operation:   "calculate",
			expectedCat: ErrorCategoryMemory,
			expectedSev: ErrorSeverityCritical,
			expectedRec: false,
		},
		{
			name:        "Timeout error",
			err:         errors.New("context deadline exceeded"),
			component:   "processor",
			operation:   "process",
			expectedCat: ErrorCategoryTimeout,
			expectedSev: ErrorSeverityMedium,
			expectedRec: true,
		},
		{
			name:        "Panic error",
			err:         errors.New("runtime error: index out of range"),
			component:   "token-processor",
			operation:   "calculate",
			expectedCat: ErrorCategoryPanic,
			expectedSev: ErrorSeverityCritical,
			expectedRec: false,
		},
		{
			name:        "Processing error",
			err:         errors.New("failed to process trade"),
			component:   "trade-processor",
			operation:   "process",
			expectedCat: ErrorCategoryProcessing,
			expectedSev: ErrorSeverityMedium,
			expectedRec: true,
		},
		{
			name:        "Configuration error",
			err:         errors.New("missing required config parameter"),
			component:   "config-loader",
			operation:   "config_load",
			expectedCat: ErrorCategoryConfiguration,
			expectedSev: ErrorSeverityHigh,
			expectedRec: false,
		},
		{
			name:        "Unknown error",
			err:         errors.New("some unknown error"),
			component:   "unknown-component",
			operation:   "unknown",
			expectedCat: ErrorCategoryUnknown,
			expectedSev: ErrorSeverityMedium,
			expectedRec: true,
		},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			catErr := categorizer.CategorizeError(tc.err, tc.component, tc.operation, nil)
			
			if catErr == nil {
				t.Fatal("CategorizeError returned nil")
			}
			
			if catErr.Category != tc.expectedCat {
				t.Errorf("Expected category %s, got %s", tc.expectedCat, catErr.Category)
			}
			
			if catErr.Severity != tc.expectedSev {
				t.Errorf("Expected severity %s, got %s", tc.expectedSev, catErr.Severity)
			}
			
			if catErr.Recoverable != tc.expectedRec {
				t.Errorf("Expected recoverable %v, got %v", tc.expectedRec, catErr.Recoverable)
			}
			
			if catErr.Component != tc.component {
				t.Errorf("Expected component %s, got %s", tc.component, catErr.Component)
			}
			
			if catErr.Operation != tc.operation {
				t.Errorf("Expected operation %s, got %s", tc.operation, catErr.Operation)
			}
			
			if catErr.Original != tc.err {
				t.Errorf("Expected original error %v, got %v", tc.err, catErr.Original)
			}
		})
	}
}

func TestCategorizeErrorWithNil(t *testing.T) {
	logger := NewLogger("test-service", "test-component")
	categorizer := NewErrorCategorizer(logger)
	
	catErr := categorizer.CategorizeError(nil, "component", "operation", nil)
	if catErr != nil {
		t.Errorf("Expected nil for nil error, got %v", catErr)
	}
}

func TestCategorizedErrorError(t *testing.T) {
	originalErr := errors.New("original error message")
	catErr := &CategorizedError{
		Original: originalErr,
		Category: ErrorCategoryRedis,
		Severity: ErrorSeverityHigh,
	}
	
	expected := "[redis:high] original error message"
	if catErr.Error() != expected {
		t.Errorf("Expected error string '%s', got '%s'", expected, catErr.Error())
	}
}

func TestCategorizedErrorUnwrap(t *testing.T) {
	originalErr := errors.New("original error message")
	catErr := &CategorizedError{
		Original: originalErr,
		Category: ErrorCategoryRedis,
		Severity: ErrorSeverityHigh,
	}
	
	if catErr.Unwrap() != originalErr {
		t.Errorf("Expected unwrapped error %v, got %v", originalErr, catErr.Unwrap())
	}
}

func TestGetRedisErrorSeverity(t *testing.T) {
	logger := NewLogger("test-service", "test-component")
	categorizer := NewErrorCategorizer(logger)
	
	testCases := []struct {
		errMsg   string
		expected ErrorSeverity
	}{
		{"redis: connection refused", ErrorSeverityCritical},
		{"redis: no route to host", ErrorSeverityCritical},
		{"redis: timeout", ErrorSeverityHigh},
		{"redis: connection reset", ErrorSeverityHigh},
		{"redis: some other error", ErrorSeverityMedium},
	}
	
	for _, tc := range testCases {
		severity := categorizer.getRedisErrorSeverity(tc.errMsg)
		if severity != tc.expected {
			t.Errorf("For error '%s', expected severity %s, got %s", tc.errMsg, tc.expected, severity)
		}
	}
}

func TestGetKafkaErrorSeverity(t *testing.T) {
	logger := NewLogger("test-service", "test-component")
	categorizer := NewErrorCategorizer(logger)
	
	testCases := []struct {
		errMsg   string
		expected ErrorSeverity
	}{
		{"kafka: no available brokers", ErrorSeverityCritical},
		{"kafka: leader not available", ErrorSeverityCritical},
		{"kafka: timeout", ErrorSeverityHigh},
		{"kafka: connection error", ErrorSeverityHigh},
		{"kafka: some other error", ErrorSeverityMedium},
	}
	
	for _, tc := range testCases {
		severity := categorizer.getKafkaErrorSeverity(tc.errMsg)
		if severity != tc.expected {
			t.Errorf("For error '%s', expected severity %s, got %s", tc.errMsg, tc.expected, severity)
		}
	}
}

func TestGetProcessingErrorSeverity(t *testing.T) {
	logger := NewLogger("test-service", "test-component")
	categorizer := NewErrorCategorizer(logger)
	
	testCases := []struct {
		errMsg    string
		operation string
		expected  ErrorSeverity
	}{
		{"data corruption detected", "process", ErrorSeverityCritical},
		{"processing failed", "critical_operation", ErrorSeverityCritical},
		{"calculation error", "calculate", ErrorSeverityHigh},
		{"aggregation failed", "aggregate", ErrorSeverityHigh},
		{"some processing error", "process", ErrorSeverityMedium},
	}
	
	for _, tc := range testCases {
		severity := categorizer.getProcessingErrorSeverity(tc.errMsg, tc.operation)
		if severity != tc.expected {
			t.Errorf("For error '%s' and operation '%s', expected severity %s, got %s", 
				tc.errMsg, tc.operation, tc.expected, severity)
		}
	}
}

func TestGetErrorStats(t *testing.T) {
	logger := NewLogger("test-service", "test-component")
	categorizer := NewErrorCategorizer(logger)
	
	stats := categorizer.GetErrorStats()
	
	// Check that stats structure is correct
	categories, ok := stats["categories"].(map[string]int)
	if !ok {
		t.Fatal("Expected categories to be map[string]int")
	}
	
	severities, ok := stats["severities"].(map[string]int)
	if !ok {
		t.Fatal("Expected severities to be map[string]int")
	}
	
	// Check that all expected categories are present
	expectedCategories := []string{
		string(ErrorCategoryRedis),
		string(ErrorCategoryKafka),
		string(ErrorCategoryNetwork),
		string(ErrorCategoryValidation),
		string(ErrorCategoryProcessing),
		string(ErrorCategoryAggregation),
		string(ErrorCategoryConfiguration),
		string(ErrorCategoryMemory),
		string(ErrorCategoryTimeout),
		string(ErrorCategoryPanic),
		string(ErrorCategoryUnknown),
	}
	
	for _, cat := range expectedCategories {
		if _, exists := categories[cat]; !exists {
			t.Errorf("Expected category '%s' to be present in stats", cat)
		}
	}
	
	// Check that all expected severities are present
	expectedSeverities := []string{
		string(ErrorSeverityLow),
		string(ErrorSeverityMedium),
		string(ErrorSeverityHigh),
		string(ErrorSeverityCritical),
	}
	
	for _, sev := range expectedSeverities {
		if _, exists := severities[sev]; !exists {
			t.Errorf("Expected severity '%s' to be present in stats", sev)
		}
	}
}