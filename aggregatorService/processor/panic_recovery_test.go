package processor

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestPanicRecovery_BasicRecovery(t *testing.T) {
	pr := NewPanicRecovery(3, 100*time.Millisecond)
	
	var panicCalled bool
	var panicInfo *PanicInfo
	
	pr.SetPanicCallback(func(info *PanicInfo) {
		panicCalled = true
		panicInfo = info
	})
	
	goroutineID := "test-goroutine-1"
	processorInfo := map[string]interface{}{
		"token": "test-token",
	}
	
	// Function that panics
	panicFunc := func() {
		panic("test panic")
	}
	
	// Run with recovery
	pr.RecoverAndRestart(goroutineID, processorInfo, panicFunc)
	
	// Wait for panic to be handled
	time.Sleep(200 * time.Millisecond)
	
	if !panicCalled {
		t.Error("Expected panic callback to be called")
	}
	
	if panicInfo == nil {
		t.Fatal("Expected panic info to be set")
	}
	
	if panicInfo.GoroutineID != goroutineID {
		t.Errorf("Expected goroutine ID %s, got %s", goroutineID, panicInfo.GoroutineID)
	}
	
	if panicInfo.PanicValue != "test panic" {
		t.Errorf("Expected panic value 'test panic', got %v", panicInfo.PanicValue)
	}
	
	if panicInfo.ProcessorInfo["token"] != "test-token" {
		t.Error("Expected processor info to be preserved")
	}
}

func TestPanicRecovery_RestartLimit(t *testing.T) {
	maxRestarts := 2
	pr := NewPanicRecovery(maxRestarts, 50*time.Millisecond)
	
	var restartCount int
	var mu sync.Mutex
	
	pr.SetRestartCallback(func(goroutineID string, attempt int) error {
		mu.Lock()
		restartCount++
		mu.Unlock()
		return nil
	})
	
	goroutineID := "test-goroutine-2"
	processorInfo := map[string]interface{}{}
	
	// Function that always panics
	panicFunc := func() {
		panic("persistent panic")
	}
	
	// Run with recovery
	pr.RecoverAndRestart(goroutineID, processorInfo, panicFunc)
	
	// Wait for all restart attempts
	time.Sleep(500 * time.Millisecond)
	
	mu.Lock()
	finalRestartCount := restartCount
	mu.Unlock()
	
	if finalRestartCount != maxRestarts {
		t.Errorf("Expected %d restart attempts, got %d", maxRestarts, finalRestartCount)
	}
	
	// Check that restart count is tracked correctly (should be maxRestarts + 1 because initial panic counts as attempt 1)
	actualRestartCount := pr.GetRestartCount(goroutineID)
	if actualRestartCount < maxRestarts {
		t.Errorf("Expected restart count at least %d, got %d", maxRestarts, actualRestartCount)
	}
}

func TestPanicRecovery_SuccessfulRestart(t *testing.T) {
	pr := NewPanicRecovery(3, 50*time.Millisecond)
	
	var executionCount int
	var mu sync.Mutex
	
	goroutineID := "test-goroutine-3"
	processorInfo := map[string]interface{}{}
	
	// Function that panics once, then succeeds
	testFunc := func() {
		mu.Lock()
		executionCount++
		count := executionCount
		mu.Unlock()
		
		if count == 1 {
			panic("first execution panic")
		}
		// Second execution succeeds (doesn't panic)
	}
	
	// Run with recovery
	pr.RecoverAndRestart(goroutineID, processorInfo, testFunc)
	
	// Wait for restart
	time.Sleep(200 * time.Millisecond)
	
	mu.Lock()
	finalCount := executionCount
	mu.Unlock()
	
	if finalCount < 2 {
		t.Errorf("Expected at least 2 executions (original + restart), got %d", finalCount)
	}
	
	if pr.GetRestartCount(goroutineID) != 1 {
		t.Errorf("Expected restart count 1, got %d", pr.GetRestartCount(goroutineID))
	}
}

func TestPanicRecovery_ResetRestartCount(t *testing.T) {
	pr := NewPanicRecovery(3, 50*time.Millisecond)
	
	goroutineID := "test-goroutine-4"
	processorInfo := map[string]interface{}{}
	
	// Simulate a panic to increment restart count
	panicFunc := func() {
		panic("test panic")
	}
	
	pr.RecoverAndRestart(goroutineID, processorInfo, panicFunc)
	time.Sleep(100 * time.Millisecond)
	
	// Check that restart count is incremented
	if pr.GetRestartCount(goroutineID) == 0 {
		t.Error("Expected restart count to be incremented")
	}
	
	// Reset restart count
	pr.ResetRestartCount(goroutineID)
	
	// Check that restart count is reset
	if pr.GetRestartCount(goroutineID) != 0 {
		t.Errorf("Expected restart count to be 0 after reset, got %d", pr.GetRestartCount(goroutineID))
	}
}

func TestPanicRecovery_GetAllRestartCounts(t *testing.T) {
	pr := NewPanicRecovery(3, 50*time.Millisecond)
	
	goroutineID1 := "test-goroutine-5"
	goroutineID2 := "test-goroutine-6"
	processorInfo := map[string]interface{}{}
	
	panicFunc := func() {
		panic("test panic")
	}
	
	// Start two goroutines that panic
	pr.RecoverAndRestart(goroutineID1, processorInfo, panicFunc)
	pr.RecoverAndRestart(goroutineID2, processorInfo, panicFunc)
	
	time.Sleep(150 * time.Millisecond)
	
	allCounts := pr.GetAllRestartCounts()
	
	if len(allCounts) != 2 {
		t.Errorf("Expected 2 goroutines in restart counts, got %d", len(allCounts))
	}
	
	if allCounts[goroutineID1] == 0 {
		t.Error("Expected restart count for goroutine 1 to be non-zero")
	}
	
	if allCounts[goroutineID2] == 0 {
		t.Error("Expected restart count for goroutine 2 to be non-zero")
	}
}

func TestSafeGoroutine(t *testing.T) {
	var panicHandled bool
	var mu sync.Mutex
	
	// Capture log output to verify panic was logged
	// In a real test, you might use a custom logger
	
	SafeGoroutine("test-safe-goroutine", func() {
		defer func() {
			// This defer will run after the panic is recovered
			mu.Lock()
			panicHandled = true
			mu.Unlock()
		}()
		panic("safe goroutine panic")
	})
	
	// Wait for goroutine to complete
	time.Sleep(100 * time.Millisecond)
	
	mu.Lock()
	handled := panicHandled
	mu.Unlock()
	
	if !handled {
		t.Error("Expected panic to be handled by SafeGoroutine")
	}
}

func TestSafeGoroutineWithContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	
	var executed bool
	var mu sync.Mutex
	
	SafeGoroutineWithContext(ctx, "test-context-goroutine", func(ctx context.Context) {
		mu.Lock()
		executed = true
		mu.Unlock()
		
		// Simulate some work
		select {
		case <-ctx.Done():
			return
		case <-time.After(50 * time.Millisecond):
			// Work completed
		}
	})
	
	// Wait for execution
	time.Sleep(150 * time.Millisecond)
	
	mu.Lock()
	wasExecuted := executed
	mu.Unlock()
	
	if !wasExecuted {
		t.Error("Expected goroutine to be executed")
	}
}

func TestErrorLogger(t *testing.T) {
	logger := NewErrorLogger("TestComponent")
	
	// Test error logging (in real tests, you'd capture log output)
	logger.LogError("test_operation", fmt.Errorf("test error"), map[string]interface{}{
		"key": "value",
	})
	
	// Test panic logging
	logger.LogPanic("test_operation", "test panic", map[string]interface{}{
		"context": "test",
	})
	
	// Test recovery logging
	logger.LogRecovery("test_operation", map[string]interface{}{
		"recovered": true,
	})
	
	// Test restart logging
	logger.LogRestart("test-goroutine", 1, map[string]interface{}{
		"attempt": 1,
	})
	
	// If we get here without panicking, the logger is working
}

func TestGetGoroutineID(t *testing.T) {
	id1 := GetGoroutineID()
	id2 := GetGoroutineID()
	
	if id1 == "" {
		t.Error("Expected non-empty goroutine ID")
	}
	
	if id2 == "" {
		t.Error("Expected non-empty goroutine ID")
	}
	
	// IDs should be the same when called from the same goroutine
	if id1 != id2 {
		t.Errorf("Expected same goroutine ID, got %s and %s", id1, id2)
	}
	
	// Test from different goroutine
	var id3 string
	var wg sync.WaitGroup
	wg.Add(1)
	
	go func() {
		defer wg.Done()
		id3 = GetGoroutineID()
	}()
	
	wg.Wait()
	
	if id3 == "" {
		t.Error("Expected non-empty goroutine ID from different goroutine")
	}
}

func TestHealthChecker_RegisterAndUpdate(t *testing.T) {
	hc := NewHealthChecker(100 * time.Millisecond)
	
	goroutineID := "test-health-1"
	metadata := map[string]interface{}{
		"component": "test",
	}
	
	// Register goroutine
	hc.RegisterGoroutine(goroutineID, metadata)
	
	// Get health info
	health, exists := hc.GetGoroutineHealth(goroutineID)
	if !exists {
		t.Fatal("Expected goroutine to be registered")
	}
	
	if health.ID != goroutineID {
		t.Errorf("Expected ID %s, got %s", goroutineID, health.ID)
	}
	
	if !health.IsHealthy {
		t.Error("Expected goroutine to be healthy initially")
	}
	
	if health.PanicCount != 0 {
		t.Errorf("Expected panic count 0, got %d", health.PanicCount)
	}
	
	// Update health
	hc.UpdateGoroutineHealth(goroutineID, false)
	
	health, exists = hc.GetGoroutineHealth(goroutineID)
	if !exists {
		t.Fatal("Expected goroutine to still be registered")
	}
	
	if health.IsHealthy {
		t.Error("Expected goroutine to be unhealthy after update")
	}
}

func TestHealthChecker_RecordPanicAndRestart(t *testing.T) {
	hc := NewHealthChecker(100 * time.Millisecond)
	
	goroutineID := "test-health-2"
	metadata := map[string]interface{}{
		"component": "test",
	}
	
	hc.RegisterGoroutine(goroutineID, metadata)
	
	// Record panic
	hc.RecordPanic(goroutineID)
	
	health, exists := hc.GetGoroutineHealth(goroutineID)
	if !exists {
		t.Fatal("Expected goroutine to be registered")
	}
	
	if health.PanicCount != 1 {
		t.Errorf("Expected panic count 1, got %d", health.PanicCount)
	}
	
	if health.IsHealthy {
		t.Error("Expected goroutine to be unhealthy after panic")
	}
	
	// Record restart
	hc.RecordRestart(goroutineID)
	
	health, exists = hc.GetGoroutineHealth(goroutineID)
	if !exists {
		t.Fatal("Expected goroutine to be registered")
	}
	
	if health.RestartCount != 1 {
		t.Errorf("Expected restart count 1, got %d", health.RestartCount)
	}
}

func TestHealthChecker_GetAllGoroutineHealth(t *testing.T) {
	hc := NewHealthChecker(100 * time.Millisecond)
	
	// Register multiple goroutines
	hc.RegisterGoroutine("test-1", map[string]interface{}{"type": "processor"})
	hc.RegisterGoroutine("test-2", map[string]interface{}{"type": "consumer"})
	
	allHealth := hc.GetAllGoroutineHealth()
	
	if len(allHealth) != 2 {
		t.Errorf("Expected 2 goroutines, got %d", len(allHealth))
	}
	
	if _, exists := allHealth["test-1"]; !exists {
		t.Error("Expected test-1 to be in health map")
	}
	
	if _, exists := allHealth["test-2"]; !exists {
		t.Error("Expected test-2 to be in health map")
	}
}

func TestHealthChecker_StartStopMonitoring(t *testing.T) {
	hc := NewHealthChecker(50 * time.Millisecond)
	
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	
	// Start monitoring
	hc.StartHealthMonitoring(ctx)
	
	// Register a goroutine
	hc.RegisterGoroutine("test-monitor", map[string]interface{}{})
	
	// Wait for monitoring to run
	time.Sleep(100 * time.Millisecond)
	
	// Stop monitoring
	hc.StopHealthMonitoring()
	
	// Test should complete without hanging
}

func TestHealthChecker_UnregisterGoroutine(t *testing.T) {
	hc := NewHealthChecker(100 * time.Millisecond)
	
	goroutineID := "test-unregister"
	hc.RegisterGoroutine(goroutineID, map[string]interface{}{})
	
	// Verify registration
	_, exists := hc.GetGoroutineHealth(goroutineID)
	if !exists {
		t.Fatal("Expected goroutine to be registered")
	}
	
	// Unregister
	hc.UnregisterGoroutine(goroutineID)
	
	// Verify unregistration
	_, exists = hc.GetGoroutineHealth(goroutineID)
	if exists {
		t.Error("Expected goroutine to be unregistered")
	}
}

func TestHealthChecker_StaleDetection(t *testing.T) {
	hc := NewHealthChecker(50 * time.Millisecond)
	
	goroutineID := "test-stale"
	hc.RegisterGoroutine(goroutineID, map[string]interface{}{})
	
	// Start monitoring
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	
	hc.StartHealthMonitoring(ctx)
	
	// Wait for stale detection (health checker uses 30 second threshold in real code)
	// For testing, we'd need to modify the threshold or use dependency injection
	time.Sleep(100 * time.Millisecond)
	
	// In a real implementation, you'd verify that stale goroutines are marked as unhealthy
	// This test demonstrates the structure but would need threshold configuration for proper testing
}

// Benchmark tests
func BenchmarkPanicRecovery_RecoverAndRestart(b *testing.B) {
	pr := NewPanicRecovery(3, 10*time.Millisecond)
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		goroutineID := fmt.Sprintf("bench-goroutine-%d", i)
		processorInfo := map[string]interface{}{
			"iteration": i,
		}
		
		normalFunc := func() {
			// Normal execution, no panic
		}
		
		pr.RecoverAndRestart(goroutineID, processorInfo, normalFunc)
	}
}

func BenchmarkGetGoroutineID(b *testing.B) {
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_ = GetGoroutineID()
	}
}

func BenchmarkHealthChecker_UpdateHealth(b *testing.B) {
	hc := NewHealthChecker(1 * time.Second)
	
	// Pre-register goroutines
	for i := 0; i < 100; i++ {
		hc.RegisterGoroutine(fmt.Sprintf("bench-%d", i), map[string]interface{}{})
	}
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		goroutineID := fmt.Sprintf("bench-%d", i%100)
		hc.UpdateGoroutineHealth(goroutineID, true)
	}
}