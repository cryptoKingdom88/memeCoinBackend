package logging

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// TraceIDKey is the context key for trace IDs
type TraceIDKey struct{}

// GenerateTraceID generates a new trace ID
func GenerateTraceID() string {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		// Fallback to timestamp-based ID if random generation fails
		return fmt.Sprintf("trace_%d", getCurrentTimestamp())
	}
	return hex.EncodeToString(bytes)
}

// WithTraceID adds a trace ID to the context
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, TraceIDKey{}, traceID)
}

// GetTraceID retrieves the trace ID from context
func GetTraceID(ctx context.Context) string {
	if traceID, ok := ctx.Value(TraceIDKey{}).(string); ok {
		return traceID
	}
	return ""
}

// GetOrGenerateTraceID gets trace ID from context or generates a new one
func GetOrGenerateTraceID(ctx context.Context) string {
	if traceID := GetTraceID(ctx); traceID != "" {
		return traceID
	}
	return GenerateTraceID()
}

// getCurrentTimestamp returns current timestamp in nanoseconds
func getCurrentTimestamp() int64 {
	return getCurrentTime().UnixNano()
}

// getCurrentTime returns current time (can be mocked for testing)
var getCurrentTime = func() time.Time {
	return time.Now()
}

// TraceableContext creates a new context with a trace ID
func TraceableContext(parent context.Context) context.Context {
	traceID := GenerateTraceID()
	return WithTraceID(parent, traceID)
}