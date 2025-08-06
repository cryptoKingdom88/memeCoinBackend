package logging

import (
	"context"
	"testing"
	"time"
)

func TestGenerateTraceID(t *testing.T) {
	traceID1 := GenerateTraceID()
	traceID2 := GenerateTraceID()
	
	// Trace IDs should not be empty
	if traceID1 == "" {
		t.Error("GenerateTraceID returned empty string")
	}
	
	if traceID2 == "" {
		t.Error("GenerateTraceID returned empty string")
	}
	
	// Trace IDs should be unique
	if traceID1 == traceID2 {
		t.Error("GenerateTraceID returned duplicate trace IDs")
	}
	
	// Trace IDs should be hex strings (32 characters for 16 bytes)
	if len(traceID1) != 32 {
		t.Errorf("Expected trace ID length 32, got %d", len(traceID1))
	}
	
	// Check that it's a valid hex string
	for _, char := range traceID1 {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f')) {
			t.Errorf("Trace ID contains invalid hex character: %c", char)
		}
	}
}

func TestWithTraceID(t *testing.T) {
	ctx := context.Background()
	traceID := "test-trace-123"
	
	ctxWithTrace := WithTraceID(ctx, traceID)
	
	// Verify trace ID is stored in context
	retrievedTraceID := GetTraceID(ctxWithTrace)
	if retrievedTraceID != traceID {
		t.Errorf("Expected trace ID '%s', got '%s'", traceID, retrievedTraceID)
	}
}

func TestGetTraceID(t *testing.T) {
	// Test with context that has no trace ID
	ctx := context.Background()
	traceID := GetTraceID(ctx)
	if traceID != "" {
		t.Errorf("Expected empty trace ID, got '%s'", traceID)
	}
	
	// Test with context that has trace ID
	expectedTraceID := "test-trace-456"
	ctxWithTrace := WithTraceID(ctx, expectedTraceID)
	retrievedTraceID := GetTraceID(ctxWithTrace)
	if retrievedTraceID != expectedTraceID {
		t.Errorf("Expected trace ID '%s', got '%s'", expectedTraceID, retrievedTraceID)
	}
}

func TestGetOrGenerateTraceID(t *testing.T) {
	// Test with context that has no trace ID - should generate new one
	ctx := context.Background()
	traceID1 := GetOrGenerateTraceID(ctx)
	if traceID1 == "" {
		t.Error("GetOrGenerateTraceID returned empty string")
	}
	
	// Test with context that has trace ID - should return existing one
	existingTraceID := "existing-trace-789"
	ctxWithTrace := WithTraceID(ctx, existingTraceID)
	traceID2 := GetOrGenerateTraceID(ctxWithTrace)
	if traceID2 != existingTraceID {
		t.Errorf("Expected existing trace ID '%s', got '%s'", existingTraceID, traceID2)
	}
}

func TestTraceableContext(t *testing.T) {
	parentCtx := context.Background()
	traceableCtx := TraceableContext(parentCtx)
	
	// Should have a trace ID
	traceID := GetTraceID(traceableCtx)
	if traceID == "" {
		t.Error("TraceableContext should generate a trace ID")
	}
	
	// Should be a valid hex string
	if len(traceID) != 32 {
		t.Errorf("Expected trace ID length 32, got %d", len(traceID))
	}
}

func TestGenerateTraceIDFallback(t *testing.T) {
	// Mock getCurrentTime to test fallback behavior
	originalGetCurrentTime := getCurrentTime
	defer func() {
		getCurrentTime = originalGetCurrentTime
	}()
	
	// Set a fixed time for testing
	fixedTime := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
	getCurrentTime = func() time.Time {
		return fixedTime
	}
	
	// This test is mainly to ensure the fallback code path exists
	// In practice, it's hard to force rand.Read to fail
	traceID := GenerateTraceID()
	if traceID == "" {
		t.Error("GenerateTraceID should not return empty string even in fallback")
	}
}

func TestTraceIDKey(t *testing.T) {
	// Test that TraceIDKey is a proper type for context keys
	key1 := TraceIDKey{}
	key2 := TraceIDKey{}
	
	// Keys should be equal (same type, no fields)
	if key1 != key2 {
		t.Error("TraceIDKey instances should be equal")
	}
	
	// Test using the key in context
	ctx := context.Background()
	traceID := "test-trace"
	
	ctxWithValue := context.WithValue(ctx, key1, traceID)
	retrievedValue := ctxWithValue.Value(key2)
	
	if retrievedValue != traceID {
		t.Errorf("Expected trace ID '%s', got '%v'", traceID, retrievedValue)
	}
}

func TestMultipleTraceIDOperations(t *testing.T) {
	ctx := context.Background()
	
	// Add first trace ID
	traceID1 := "trace-1"
	ctx1 := WithTraceID(ctx, traceID1)
	
	// Verify first trace ID
	if GetTraceID(ctx1) != traceID1 {
		t.Errorf("Expected trace ID '%s', got '%s'", traceID1, GetTraceID(ctx1))
	}
	
	// Override with second trace ID
	traceID2 := "trace-2"
	ctx2 := WithTraceID(ctx1, traceID2)
	
	// Verify second trace ID
	if GetTraceID(ctx2) != traceID2 {
		t.Errorf("Expected trace ID '%s', got '%s'", traceID2, GetTraceID(ctx2))
	}
	
	// Original context should still have first trace ID
	if GetTraceID(ctx1) != traceID1 {
		t.Errorf("Original context should still have trace ID '%s', got '%s'", traceID1, GetTraceID(ctx1))
	}
}