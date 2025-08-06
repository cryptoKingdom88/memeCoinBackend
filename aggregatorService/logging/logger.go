package logging

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"time"
)

// LogLevel represents the severity level of a log entry
type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
	FATAL
)

// String returns the string representation of the log level
func (l LogLevel) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	case FATAL:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

// LogEntry represents a structured log entry
type LogEntry struct {
	Timestamp   time.Time              `json:"timestamp"`
	Level       string                 `json:"level"`
	Message     string                 `json:"message"`
	Service     string                 `json:"service"`
	Component   string                 `json:"component,omitempty"`
	TraceID     string                 `json:"trace_id,omitempty"`
	TokenAddress string                `json:"token_address,omitempty"`
	Operation   string                 `json:"operation,omitempty"`
	Duration    string                 `json:"duration,omitempty"`
	Error       string                 `json:"error,omitempty"`
	Fields      map[string]interface{} `json:"fields,omitempty"`
	Caller      string                 `json:"caller,omitempty"`
}

// Logger provides structured logging functionality
type Logger struct {
	level     LogLevel
	service   string
	component string
	output    *log.Logger
	context   map[string]interface{}
}

// NewLogger creates a new structured logger
func NewLogger(service, component string) *Logger {
	level := getLogLevelFromEnv()
	
	return &Logger{
		level:     level,
		service:   service,
		component: component,
		output:    log.New(os.Stdout, "", 0),
		context:   make(map[string]interface{}),
	}
}

// WithContext returns a new logger with additional context fields
func (l *Logger) WithContext(fields map[string]interface{}) *Logger {
	newLogger := &Logger{
		level:     l.level,
		service:   l.service,
		component: l.component,
		output:    l.output,
		context:   make(map[string]interface{}),
	}
	
	// Copy existing context
	for k, v := range l.context {
		newLogger.context[k] = v
	}
	
	// Add new fields
	for k, v := range fields {
		newLogger.context[k] = v
	}
	
	return newLogger
}

// WithTraceID returns a new logger with trace ID context
func (l *Logger) WithTraceID(traceID string) *Logger {
	return l.WithContext(map[string]interface{}{
		"trace_id": traceID,
	})
}

// WithToken returns a new logger with token address context
func (l *Logger) WithToken(tokenAddress string) *Logger {
	return l.WithContext(map[string]interface{}{
		"token_address": tokenAddress,
	})
}

// WithOperation returns a new logger with operation context
func (l *Logger) WithOperation(operation string) *Logger {
	return l.WithContext(map[string]interface{}{
		"operation": operation,
	})
}

// WithError returns a new logger with error context
func (l *Logger) WithError(err error) *Logger {
	if err == nil {
		return l
	}
	return l.WithContext(map[string]interface{}{
		"error": err.Error(),
	})
}

// Debug logs a debug message
func (l *Logger) Debug(message string, fields ...map[string]interface{}) {
	l.log(DEBUG, message, fields...)
}

// Info logs an info message
func (l *Logger) Info(message string, fields ...map[string]interface{}) {
	l.log(INFO, message, fields...)
}

// Warn logs a warning message
func (l *Logger) Warn(message string, fields ...map[string]interface{}) {
	l.log(WARN, message, fields...)
}

// Error logs an error message
func (l *Logger) Error(message string, fields ...map[string]interface{}) {
	l.log(ERROR, message, fields...)
}

// Fatal logs a fatal message and exits
func (l *Logger) Fatal(message string, fields ...map[string]interface{}) {
	l.log(FATAL, message, fields...)
	os.Exit(1)
}

// TradeProcessed logs trade processing events
func (l *Logger) TradeProcessed(tokenAddress, traceID string, duration time.Duration, success bool) {
	level := INFO
	message := "Trade processed successfully"
	if !success {
		level = ERROR
		message = "Trade processing failed"
	}
	
	l.WithTraceID(traceID).WithToken(tokenAddress).log(level, message, map[string]interface{}{
		"operation": "trade_processing",
		"duration":  duration.String(),
		"success":   success,
	})
}

// AggregationCalculated logs aggregation calculation events
func (l *Logger) AggregationCalculated(tokenAddress, timeframe, traceID string, duration time.Duration, success bool) {
	level := INFO
	message := "Aggregation calculated successfully"
	if !success {
		level = ERROR
		message = "Aggregation calculation failed"
	}
	
	l.WithTraceID(traceID).WithToken(tokenAddress).log(level, message, map[string]interface{}{
		"operation": "aggregation_calculation",
		"timeframe": timeframe,
		"duration":  duration.String(),
		"success":   success,
	})
}

// RedisOperation logs Redis operation events
func (l *Logger) RedisOperation(operation, traceID string, duration time.Duration, success bool, err error) {
	level := DEBUG
	message := fmt.Sprintf("Redis %s operation completed", operation)
	
	fields := map[string]interface{}{
		"operation": fmt.Sprintf("redis_%s", strings.ToLower(operation)),
		"duration":  duration.String(),
		"success":   success,
	}
	
	if !success {
		level = ERROR
		message = fmt.Sprintf("Redis %s operation failed", operation)
		if err != nil {
			fields["error"] = err.Error()
		}
	}
	
	l.WithTraceID(traceID).log(level, message, fields)
}

// MaintenanceEvent logs maintenance service events
func (l *Logger) MaintenanceEvent(event, traceID string, details map[string]interface{}) {
	fields := map[string]interface{}{
		"operation": "maintenance",
		"event":     event,
	}
	
	// Merge details
	for k, v := range details {
		fields[k] = v
	}
	
	l.WithTraceID(traceID).Info(fmt.Sprintf("Maintenance event: %s", event), fields)
}

// WorkerPoolEvent logs worker pool events
func (l *Logger) WorkerPoolEvent(event string, workerCount, queueSize int) {
	l.Info(fmt.Sprintf("Worker pool event: %s", event), map[string]interface{}{
		"operation":    "worker_pool",
		"event":        event,
		"worker_count": workerCount,
		"queue_size":   queueSize,
	})
}

// SystemEvent logs system-level events
func (l *Logger) SystemEvent(event string, details map[string]interface{}) {
	fields := map[string]interface{}{
		"operation": "system",
		"event":     event,
	}
	
	// Merge details
	for k, v := range details {
		fields[k] = v
	}
	
	l.Info(fmt.Sprintf("System event: %s", event), fields)
}

// PanicRecovery logs panic recovery events
func (l *Logger) PanicRecovery(tokenAddress, component string, panicValue interface{}, stackTrace string) {
	l.WithToken(tokenAddress).Error("Panic recovered", map[string]interface{}{
		"operation":   "panic_recovery",
		"component":   component,
		"panic_value": fmt.Sprintf("%v", panicValue),
		"stack_trace": stackTrace,
	})
}

// log is the internal logging method
func (l *Logger) log(level LogLevel, message string, fields ...map[string]interface{}) {
	if level < l.level {
		return
	}
	
	entry := LogEntry{
		Timestamp: time.Now().UTC(),
		Level:     level.String(),
		Message:   message,
		Service:   l.service,
		Component: l.component,
		Fields:    make(map[string]interface{}),
	}
	
	// Add caller information for ERROR and FATAL levels
	if level >= ERROR {
		if pc, file, line, ok := runtime.Caller(2); ok {
			if fn := runtime.FuncForPC(pc); fn != nil {
				entry.Caller = fmt.Sprintf("%s:%d %s", file, line, fn.Name())
			}
		}
	}
	
	// Merge context fields
	for k, v := range l.context {
		switch k {
		case "trace_id":
			if traceID, ok := v.(string); ok {
				entry.TraceID = traceID
			}
		case "token_address":
			if tokenAddr, ok := v.(string); ok {
				entry.TokenAddress = tokenAddr
			}
		case "operation":
			if op, ok := v.(string); ok {
				entry.Operation = op
			}
		case "error":
			if errStr, ok := v.(string); ok {
				entry.Error = errStr
			}
		default:
			entry.Fields[k] = v
		}
	}
	
	// Merge additional fields
	for _, fieldMap := range fields {
		for k, v := range fieldMap {
			switch k {
			case "duration":
				if dur, ok := v.(string); ok {
					entry.Duration = dur
				}
			case "error":
				if errStr, ok := v.(string); ok {
					entry.Error = errStr
				}
			default:
				entry.Fields[k] = v
			}
		}
	}
	
	// Remove empty fields map
	if len(entry.Fields) == 0 {
		entry.Fields = nil
	}
	
	// Marshal to JSON and output
	if jsonBytes, err := json.Marshal(entry); err == nil {
		l.output.Println(string(jsonBytes))
	} else {
		// Fallback to simple logging if JSON marshaling fails
		l.output.Printf("[%s] %s %s: %s\n", entry.Timestamp.Format(time.RFC3339), entry.Level, entry.Service, message)
	}
}

// getLogLevelFromEnv gets log level from environment variable
func getLogLevelFromEnv() LogLevel {
	levelStr := strings.ToUpper(os.Getenv("LOG_LEVEL"))
	switch levelStr {
	case "DEBUG":
		return DEBUG
	case "INFO":
		return INFO
	case "WARN", "WARNING":
		return WARN
	case "ERROR":
		return ERROR
	case "FATAL":
		return FATAL
	default:
		return INFO // Default to INFO level
	}
}