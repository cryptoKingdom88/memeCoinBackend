package logging

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"time"
)

// LogLevel represents the severity level of a log entry
type LogLevel string

const (
	DEBUG LogLevel = "DEBUG"
	INFO  LogLevel = "INFO"
	WARN  LogLevel = "WARN"
	ERROR LogLevel = "ERROR"
	FATAL LogLevel = "FATAL"
)

// LogEntry represents a structured log entry
type LogEntry struct {
	Timestamp string                 `json:"timestamp"`
	Level     LogLevel               `json:"level"`
	Message   string                 `json:"message"`
	Service   string                 `json:"service"`
	Component string                 `json:"component,omitempty"`
	TraceID   string                 `json:"trace_id,omitempty"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
	File      string                 `json:"file,omitempty"`
	Line      int                    `json:"line,omitempty"`
}

// Logger provides structured logging functionality
type Logger struct {
	output    io.Writer
	service   string
	component string
	level     LogLevel
	traceID   string
	fields    map[string]interface{}
}

// NewLogger creates a new structured logger
func NewLogger(service string) *Logger {
	return &Logger{
		output:  os.Stdout,
		service: service,
		level:   INFO,
		fields:  make(map[string]interface{}),
	}
}

// SetOutput sets the output destination for the logger
func (l *Logger) SetOutput(w io.Writer) *Logger {
	l.output = w
	return l
}

// SetLevel sets the minimum log level
func (l *Logger) SetLevel(level LogLevel) *Logger {
	l.level = level
	return l
}

// WithComponent returns a new logger with the specified component
func (l *Logger) WithComponent(component string) *Logger {
	newLogger := *l
	newLogger.component = component
	newLogger.fields = make(map[string]interface{})
	for k, v := range l.fields {
		newLogger.fields[k] = v
	}
	return &newLogger
}

// WithTraceID returns a new logger with the specified trace ID
func (l *Logger) WithTraceID(traceID string) *Logger {
	newLogger := *l
	newLogger.traceID = traceID
	newLogger.fields = make(map[string]interface{})
	for k, v := range l.fields {
		newLogger.fields[k] = v
	}
	return &newLogger
}

// WithField returns a new logger with an additional field
func (l *Logger) WithField(key string, value interface{}) *Logger {
	newLogger := *l
	newLogger.fields = make(map[string]interface{})
	for k, v := range l.fields {
		newLogger.fields[k] = v
	}
	newLogger.fields[key] = value
	return &newLogger
}

// WithFields returns a new logger with additional fields
func (l *Logger) WithFields(fields map[string]interface{}) *Logger {
	newLogger := *l
	newLogger.fields = make(map[string]interface{})
	for k, v := range l.fields {
		newLogger.fields[k] = v
	}
	for k, v := range fields {
		newLogger.fields[k] = v
	}
	return &newLogger
}

// shouldLog determines if a message should be logged based on level
func (l *Logger) shouldLog(level LogLevel) bool {
	levels := map[LogLevel]int{
		DEBUG: 0,
		INFO:  1,
		WARN:  2,
		ERROR: 3,
		FATAL: 4,
	}
	return levels[level] >= levels[l.level]
}

// log writes a log entry
func (l *Logger) log(level LogLevel, message string) {
	if !l.shouldLog(level) {
		return
	}

	entry := LogEntry{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Level:     level,
		Message:   message,
		Service:   l.service,
		Component: l.component,
		TraceID:   l.traceID,
		Fields:    l.fields,
	}

	// Add file and line information for ERROR and FATAL levels
	if level == ERROR || level == FATAL {
		if _, file, line, ok := runtime.Caller(2); ok {
			entry.File = file
			entry.Line = line
		}
	}

	// Marshal to JSON
	jsonData, err := json.Marshal(entry)
	if err != nil {
		// Fallback to standard logging if JSON marshaling fails
		log.Printf("Failed to marshal log entry: %v", err)
		log.Printf("[%s] %s: %s", level, l.service, message)
		return
	}

	// Write to output
	fmt.Fprintln(l.output, string(jsonData))

	// Exit for FATAL level
	if level == FATAL {
		os.Exit(1)
	}
}

// Debug logs a debug message
func (l *Logger) Debug(message string) {
	l.log(DEBUG, message)
}

// Debugf logs a formatted debug message
func (l *Logger) Debugf(format string, args ...interface{}) {
	l.log(DEBUG, fmt.Sprintf(format, args...))
}

// Info logs an info message
func (l *Logger) Info(message string) {
	l.log(INFO, message)
}

// Infof logs a formatted info message
func (l *Logger) Infof(format string, args ...interface{}) {
	l.log(INFO, fmt.Sprintf(format, args...))
}

// Warn logs a warning message
func (l *Logger) Warn(message string) {
	l.log(WARN, message)
}

// Warnf logs a formatted warning message
func (l *Logger) Warnf(format string, args ...interface{}) {
	l.log(WARN, fmt.Sprintf(format, args...))
}

// Error logs an error message
func (l *Logger) Error(message string) {
	l.log(ERROR, message)
}

// Errorf logs a formatted error message
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.log(ERROR, fmt.Sprintf(format, args...))
}

// Fatal logs a fatal message and exits
func (l *Logger) Fatal(message string) {
	l.log(FATAL, message)
}

// Fatalf logs a formatted fatal message and exits
func (l *Logger) Fatalf(format string, args ...interface{}) {
	l.log(FATAL, fmt.Sprintf(format, args...))
}

// Global logger instance
var defaultLogger = NewLogger("backend-websocket-service")

// SetGlobalLevel sets the global logger level
func SetGlobalLevel(level LogLevel) {
	defaultLogger.SetLevel(level)
}

// SetGlobalOutput sets the global logger output
func SetGlobalOutput(w io.Writer) {
	defaultLogger.SetOutput(w)
}

// GetLogger returns the default logger
func GetLogger() *Logger {
	return defaultLogger
}

// InitFromEnv initializes logging from environment variables
func InitFromEnv() {
	// Set log level from environment
	if levelStr := os.Getenv("LOG_LEVEL"); levelStr != "" {
		level := LogLevel(strings.ToUpper(levelStr))
		switch level {
		case DEBUG, INFO, WARN, ERROR, FATAL:
			SetGlobalLevel(level)
		default:
			defaultLogger.Warnf("Invalid LOG_LEVEL: %s, using INFO", levelStr)
		}
	}

	// Set log format from environment
	if format := os.Getenv("LOG_FORMAT"); format == "text" {
		// For development, we might want text format
		// This is a simple implementation - in production, use JSON
		defaultLogger.Info("Using structured JSON logging")
	}
}