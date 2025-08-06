package processor

import (
	"context"
	"testing"
	"time"
)

func TestTokenProcessor_DataConsistencyIntegration(t *testing.T) {
	// Create mock dependencies
	redisManager := NewMockRedisManager()
	calculator := NewMockSlidingWindowCalculator()
	
	// Create token processor
	processor := NewTokenProcessor()
	
	// Initialize processor
	tokenAddress := "consistency-test-token"
	err := processor.Initialize(tokenAddress, redisManager, calculator)
	if err != nil {
		t.Fatalf("Failed to initialize processor: %v", err)
	}
	
	// Check that consistency validator is initialized
	if tp, ok := processor.(*TokenProcessor); ok {
		if tp.consistencyValidator == nil {
			t.Fatal("Expected consistency validator to be initialized")
		}
		
		// Test validation needed check
		if !tp.IsValidationNeeded() {
			t.Error("Expected validation to be needed initially")
		}
		
		// Perform validation
		ctx := context.Background()
		result, err := tp.ValidateDataConsistency(ctx)
		if err != nil {
			t.Fatalf("Data consistency validation failed: %v", err)
		}
		
		if result == nil {
			t.Fatal("Expected validation result")
		}
		
		if result.TokenAddress != tokenAddress {
			t.Errorf("Expected token address %s, got %s", tokenAddress, result.TokenAddress)
		}
		
		// Check that validation time is updated
		lastValidation := tp.GetLastValidationTime()
		if lastValidation.IsZero() {
			t.Error("Expected last validation time to be set")
		}
		
		// Should not need validation immediately after
		if tp.IsValidationNeeded() {
			t.Error("Expected validation to not be needed immediately after validation")
		}
		
		// Test recovery from Redis
		err = tp.RecoverFromRedis(ctx)
		if err != nil {
			t.Errorf("Recovery from Redis failed: %v", err)
		}
		
		t.Logf("Data consistency validation completed successfully")
		t.Logf("Validation result: consistent=%v, issues=%d, recovery_applied=%v", 
			result.IsConsistent, len(result.Issues), result.RecoveryApplied)
	}
	
	// Shutdown processor
	err = processor.Shutdown(context.Background())
	if err != nil {
		t.Errorf("Failed to shutdown processor: %v", err)
	}
}

func TestTokenProcessor_ValidationNeededLogic(t *testing.T) {
	// Create mock dependencies
	redisManager := NewMockRedisManager()
	calculator := NewMockSlidingWindowCalculator()
	
	// Create token processor
	processor := NewTokenProcessor()
	
	// Initialize processor
	tokenAddress := "validation-logic-test-token"
	err := processor.Initialize(tokenAddress, redisManager, calculator)
	if err != nil {
		t.Fatalf("Failed to initialize processor: %v", err)
	}
	
	if tp, ok := processor.(*TokenProcessor); ok {
		// Should need validation initially
		if !tp.IsValidationNeeded() {
			t.Error("Expected validation to be needed initially")
		}
		
		// Simulate validation by setting last validation time
		tp.validationMutex.Lock()
		tp.lastValidationTime = time.Now()
		tp.validationMutex.Unlock()
		
		// Should not need validation immediately after
		if tp.IsValidationNeeded() {
			t.Error("Expected validation to not be needed immediately after validation")
		}
		
		// Simulate time passing (more than 5 minutes)
		tp.validationMutex.Lock()
		tp.lastValidationTime = time.Now().Add(-6 * time.Minute)
		tp.validationMutex.Unlock()
		
		// Should need validation after 5 minutes
		if !tp.IsValidationNeeded() {
			t.Error("Expected validation to be needed after 5 minutes")
		}
	}
	
	// Shutdown processor
	err = processor.Shutdown(context.Background())
	if err != nil {
		t.Errorf("Failed to shutdown processor: %v", err)
	}
}