package logging

import (
	"fmt"
	"strings"
)

// ErrorCategory represents different categories of errors
type ErrorCategory string

const (
	// Infrastructure errors
	ErrorCategoryRedis     ErrorCategory = "redis"
	ErrorCategoryKafka     ErrorCategory = "kafka"
	ErrorCategoryNetwork   ErrorCategory = "network"
	ErrorCategoryDatabase  ErrorCategory = "database"
	
	// Application errors
	ErrorCategoryValidation    ErrorCategory = "validation"
	ErrorCategoryProcessing    ErrorCategory = "processing"
	ErrorCategoryAggregation   ErrorCategory = "aggregation"
	ErrorCategoryConfiguration ErrorCategory = "configuration"
	
	// System errors
	ErrorCategoryMemory    ErrorCategory = "memory"
	ErrorCategoryTimeout   ErrorCategory = "timeout"
	ErrorCategoryPanic     ErrorCategory = "panic"
	ErrorCategoryUnknown   ErrorCategory = "unknown"
)

// ErrorSeverity represents the severity level of an error
type ErrorSeverity string

const (
	ErrorSeverityLow      ErrorSeverity = "low"
	ErrorSeverityMedium   ErrorSeverity = "medium"
	ErrorSeverityHigh     ErrorSeverity = "high"
	ErrorSeverityCritical ErrorSeverity = "critical"
)

// CategorizedError represents an error with category and severity
type CategorizedError struct {
	Original    error
	Category    ErrorCategory
	Severity    ErrorSeverity
	Component   string
	Operation   string
	Context     map[string]interface{}
	Recoverable bool
}

// Error implements the error interface
func (ce *CategorizedError) Error() string {
	return fmt.Sprintf("[%s:%s] %s", ce.Category, ce.Severity, ce.Original.Error())
}

// Unwrap returns the original error
func (ce *CategorizedError) Unwrap() error {
	return ce.Original
}

// ErrorCategorizer provides error categorization functionality
type ErrorCategorizer struct {
	logger *Logger
}

// NewErrorCategorizer creates a new error categorizer
func NewErrorCategorizer(logger *Logger) *ErrorCategorizer {
	return &ErrorCategorizer{
		logger: logger,
	}
}

// CategorizeError categorizes an error based on its content and context
func (ec *ErrorCategorizer) CategorizeError(err error, component, operation string, context map[string]interface{}) *CategorizedError {
	if err == nil {
		return nil
	}
	
	errMsg := strings.ToLower(err.Error())
	category, severity, recoverable := ec.analyzeError(errMsg, component, operation)
	
	return &CategorizedError{
		Original:    err,
		Category:    category,
		Severity:    severity,
		Component:   component,
		Operation:   operation,
		Context:     context,
		Recoverable: recoverable,
	}
}

// LogCategorizedError logs a categorized error with appropriate context
func (ec *ErrorCategorizer) LogCategorizedError(catErr *CategorizedError, traceID string) {
	if catErr == nil {
		return
	}
	
	fields := map[string]interface{}{
		"error_category":   string(catErr.Category),
		"error_severity":   string(catErr.Severity),
		"error_component":  catErr.Component,
		"error_operation":  catErr.Operation,
		"error_recoverable": catErr.Recoverable,
		"original_error":   catErr.Original.Error(),
	}
	
	// Add context fields
	for k, v := range catErr.Context {
		fields[fmt.Sprintf("ctx_%s", k)] = v
	}
	
	// Log with appropriate level based on severity
	logger := ec.logger.WithTraceID(traceID)
	switch catErr.Severity {
	case ErrorSeverityLow:
		logger.Warn("Categorized error occurred", fields)
	case ErrorSeverityMedium:
		logger.Error("Categorized error occurred", fields)
	case ErrorSeverityHigh, ErrorSeverityCritical:
		logger.Error("Critical categorized error occurred", fields)
	}
}

// analyzeError analyzes error message and context to determine category and severity
func (ec *ErrorCategorizer) analyzeError(errMsg, component, operation string) (ErrorCategory, ErrorSeverity, bool) {
	// Redis errors
	if strings.Contains(errMsg, "redis") || strings.Contains(errMsg, "connection refused") ||
		strings.Contains(errMsg, "connection reset") || strings.Contains(errMsg, "timeout") && strings.Contains(component, "redis") {
		return ErrorCategoryRedis, ec.getRedisErrorSeverity(errMsg), true
	}
	
	// Kafka errors
	if strings.Contains(errMsg, "kafka") || strings.Contains(errMsg, "broker") ||
		strings.Contains(errMsg, "consumer") && strings.Contains(component, "kafka") {
		return ErrorCategoryKafka, ec.getKafkaErrorSeverity(errMsg), true
	}
	
	// Network errors
	if strings.Contains(errMsg, "network") || strings.Contains(errMsg, "connection") ||
		strings.Contains(errMsg, "dial") || strings.Contains(errMsg, "no route to host") {
		return ErrorCategoryNetwork, ErrorSeverityMedium, true
	}
	
	// Validation errors
	if strings.Contains(errMsg, "invalid") || strings.Contains(errMsg, "validation") ||
		strings.Contains(errMsg, "parse") || strings.Contains(errMsg, "unmarshal") {
		return ErrorCategoryValidation, ErrorSeverityLow, false
	}
	
	// Memory errors
	if strings.Contains(errMsg, "out of memory") || strings.Contains(errMsg, "memory") ||
		strings.Contains(errMsg, "allocation") {
		return ErrorCategoryMemory, ErrorSeverityCritical, false
	}
	
	// Timeout errors
	if strings.Contains(errMsg, "timeout") || strings.Contains(errMsg, "deadline exceeded") {
		return ErrorCategoryTimeout, ErrorSeverityMedium, true
	}
	
	// Panic errors
	if strings.Contains(errMsg, "panic") || strings.Contains(errMsg, "runtime error") {
		return ErrorCategoryPanic, ErrorSeverityCritical, false
	}
	
	// Processing errors based on component
	if strings.Contains(component, "processor") || strings.Contains(component, "aggregat") {
		return ErrorCategoryProcessing, ec.getProcessingErrorSeverity(errMsg, operation), true
	}
	
	// Configuration errors
	if strings.Contains(errMsg, "config") || strings.Contains(errMsg, "environment") ||
		strings.Contains(errMsg, "missing") && strings.Contains(operation, "config") {
		return ErrorCategoryConfiguration, ErrorSeverityHigh, false
	}
	
	// Default to unknown
	return ErrorCategoryUnknown, ErrorSeverityMedium, true
}

// getRedisErrorSeverity determines severity for Redis errors
func (ec *ErrorCategorizer) getRedisErrorSeverity(errMsg string) ErrorSeverity {
	if strings.Contains(errMsg, "connection refused") || strings.Contains(errMsg, "no route to host") {
		return ErrorSeverityCritical
	}
	if strings.Contains(errMsg, "timeout") || strings.Contains(errMsg, "connection reset") {
		return ErrorSeverityHigh
	}
	return ErrorSeverityMedium
}

// getKafkaErrorSeverity determines severity for Kafka errors
func (ec *ErrorCategorizer) getKafkaErrorSeverity(errMsg string) ErrorSeverity {
	if strings.Contains(errMsg, "no available brokers") || strings.Contains(errMsg, "leader not available") {
		return ErrorSeverityCritical
	}
	if strings.Contains(errMsg, "timeout") || strings.Contains(errMsg, "connection") {
		return ErrorSeverityHigh
	}
	return ErrorSeverityMedium
}

// getProcessingErrorSeverity determines severity for processing errors
func (ec *ErrorCategorizer) getProcessingErrorSeverity(errMsg, operation string) ErrorSeverity {
	if strings.Contains(operation, "critical") || strings.Contains(errMsg, "data corruption") {
		return ErrorSeverityCritical
	}
	if strings.Contains(errMsg, "calculation") || strings.Contains(errMsg, "aggregation") {
		return ErrorSeverityHigh
	}
	return ErrorSeverityMedium
}

// GetErrorStats returns error statistics by category and severity
func (ec *ErrorCategorizer) GetErrorStats() map[string]interface{} {
	// This would typically be implemented with persistent storage
	// For now, return a placeholder structure
	return map[string]interface{}{
		"categories": map[string]int{
			string(ErrorCategoryRedis):        0,
			string(ErrorCategoryKafka):        0,
			string(ErrorCategoryNetwork):      0,
			string(ErrorCategoryValidation):   0,
			string(ErrorCategoryProcessing):   0,
			string(ErrorCategoryAggregation):  0,
			string(ErrorCategoryConfiguration): 0,
			string(ErrorCategoryMemory):       0,
			string(ErrorCategoryTimeout):      0,
			string(ErrorCategoryPanic):        0,
			string(ErrorCategoryUnknown):      0,
		},
		"severities": map[string]int{
			string(ErrorSeverityLow):      0,
			string(ErrorSeverityMedium):   0,
			string(ErrorSeverityHigh):     0,
			string(ErrorSeverityCritical): 0,
		},
	}
}