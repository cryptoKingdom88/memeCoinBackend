package processor

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"runtime/debug"
	"sync"
	"time"
)

// PanicRecovery handles panic recovery and goroutine restart functionality
type PanicRecovery struct {
	restartAttempts map[string]int
	maxRestarts     int
	restartDelay    time.Duration
	mutex           sync.RWMutex
	
	// Callbacks for restart logic
	onPanic   func(panicInfo *PanicInfo)
	onRestart func(goroutineID string, attempt int) error
}

// PanicInfo contains information about a panic
type PanicInfo struct {
	GoroutineID   string
	PanicValue    interface{}
	StackTrace    string
	Timestamp     time.Time
	RestartCount  int
	ProcessorInfo map[string]interface{}
}

// NewPanicRecovery creates a new panic recovery manager
func NewPanicRecovery(maxRestarts int, restartDelay time.Duration) *PanicRecovery {
	return &PanicRecovery{
		restartAttempts: make(map[string]int),
		maxRestarts:     maxRestarts,
		restartDelay:    restartDelay,
	}
}

// SetPanicCallback sets the callback function to be called when a panic occurs
func (pr *PanicRecovery) SetPanicCallback(callback func(panicInfo *PanicInfo)) {
	pr.onPanic = callback
}

// SetRestartCallback sets the callback function to be called when restarting a goroutine
func (pr *PanicRecovery) SetRestartCallback(callback func(goroutineID string, attempt int) error) {
	pr.onRestart = callback
}

// RecoverAndRestart wraps a goroutine function with panic recovery and restart logic
func (pr *PanicRecovery) RecoverAndRestart(goroutineID string, processorInfo map[string]interface{}, fn func()) {
	go pr.runWithRecovery(goroutineID, processorInfo, fn)
}

// runWithRecovery runs a function with panic recovery
func (pr *PanicRecovery) runWithRecovery(goroutineID string, processorInfo map[string]interface{}, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			// Capture panic information
			panicInfo := &PanicInfo{
				GoroutineID:   goroutineID,
				PanicValue:    r,
				StackTrace:    string(debug.Stack()),
				Timestamp:     time.Now(),
				ProcessorInfo: processorInfo,
			}
			
			pr.mutex.Lock()
			panicInfo.RestartCount = pr.restartAttempts[goroutineID]
			pr.mutex.Unlock()
			
			// Log panic details
			log.Printf("PANIC in goroutine %s: %v", goroutineID, r)
			log.Printf("Stack trace:\n%s", panicInfo.StackTrace)
			log.Printf("Processor info: %+v", processorInfo)
			
			// Call panic callback if set
			if pr.onPanic != nil {
				pr.onPanic(panicInfo)
			}
			
			// Attempt restart
			pr.attemptRestart(goroutineID, processorInfo, fn)
		}
	}()
	
	// Run the actual function
	fn()
}

// attemptRestart attempts to restart a failed goroutine
func (pr *PanicRecovery) attemptRestart(goroutineID string, processorInfo map[string]interface{}, fn func()) {
	pr.mutex.Lock()
	attempts := pr.restartAttempts[goroutineID]
	pr.restartAttempts[goroutineID] = attempts + 1
	pr.mutex.Unlock()
	
	if attempts >= pr.maxRestarts {
		log.Printf("Maximum restart attempts (%d) reached for goroutine %s, giving up", pr.maxRestarts, goroutineID)
		return
	}
	
	log.Printf("Attempting to restart goroutine %s (attempt %d/%d)", goroutineID, attempts+1, pr.maxRestarts)
	
	// Call restart callback if set
	if pr.onRestart != nil {
		if err := pr.onRestart(goroutineID, attempts+1); err != nil {
			log.Printf("Restart callback failed for goroutine %s: %v", goroutineID, err)
			return
		}
	}
	
	// Wait before restart to prevent rapid restart loops
	time.Sleep(pr.restartDelay)
	
	// Restart the goroutine
	go pr.runWithRecovery(goroutineID, processorInfo, fn)
	
	log.Printf("Goroutine %s restarted successfully (attempt %d)", goroutineID, attempts+1)
}

// ResetRestartCount resets the restart count for a goroutine
func (pr *PanicRecovery) ResetRestartCount(goroutineID string) {
	pr.mutex.Lock()
	defer pr.mutex.Unlock()
	delete(pr.restartAttempts, goroutineID)
}

// GetRestartCount returns the current restart count for a goroutine
func (pr *PanicRecovery) GetRestartCount(goroutineID string) int {
	pr.mutex.RLock()
	defer pr.mutex.RUnlock()
	return pr.restartAttempts[goroutineID]
}

// GetAllRestartCounts returns restart counts for all goroutines
func (pr *PanicRecovery) GetAllRestartCounts() map[string]int {
	pr.mutex.RLock()
	defer pr.mutex.RUnlock()
	
	result := make(map[string]int)
	for k, v := range pr.restartAttempts {
		result[k] = v
	}
	return result
}

// SafeGoroutine is a utility function to run a goroutine with basic panic recovery
func SafeGoroutine(name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC in goroutine %s: %v", name, r)
				log.Printf("Stack trace:\n%s", string(debug.Stack()))
			}
		}()
		fn()
	}()
}

// SafeGoroutineWithContext runs a goroutine with panic recovery and context support
func SafeGoroutineWithContext(ctx context.Context, name string, fn func(context.Context)) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC in goroutine %s: %v", name, r)
				log.Printf("Stack trace:\n%s", string(debug.Stack()))
			}
		}()
		fn(ctx)
	}()
}

// ErrorLogger provides structured error logging with context
type ErrorLogger struct {
	component string
}

// NewErrorLogger creates a new error logger for a component
func NewErrorLogger(component string) *ErrorLogger {
	return &ErrorLogger{component: component}
}

// LogError logs an error with context information
func (el *ErrorLogger) LogError(operation string, err error, context map[string]interface{}) {
	log.Printf("[%s] Error in %s: %v", el.component, operation, err)
	if context != nil {
		log.Printf("[%s] Context: %+v", el.component, context)
	}
}

// LogPanic logs a panic with detailed information
func (el *ErrorLogger) LogPanic(operation string, panicValue interface{}, context map[string]interface{}) {
	log.Printf("[%s] PANIC in %s: %v", el.component, operation, panicValue)
	log.Printf("[%s] Stack trace:\n%s", el.component, string(debug.Stack()))
	if context != nil {
		log.Printf("[%s] Context: %+v", el.component, context)
	}
}

// LogRecovery logs a successful recovery from panic
func (el *ErrorLogger) LogRecovery(operation string, context map[string]interface{}) {
	log.Printf("[%s] Successfully recovered from panic in %s", el.component, operation)
	if context != nil {
		log.Printf("[%s] Context: %+v", el.component, context)
	}
}

// LogRestart logs a goroutine restart
func (el *ErrorLogger) LogRestart(goroutineID string, attempt int, context map[string]interface{}) {
	log.Printf("[%s] Restarting goroutine %s (attempt %d)", el.component, goroutineID, attempt)
	if context != nil {
		log.Printf("[%s] Context: %+v", el.component, context)
	}
}

// GetGoroutineID returns a unique identifier for the current goroutine
func GetGoroutineID() string {
	buf := make([]byte, 64)
	buf = buf[:runtime.Stack(buf, false)]
	// Extract goroutine ID from stack trace
	// Format: "goroutine 123 [running]:"
	for i := 0; i < len(buf); i++ {
		if buf[i] == ' ' {
			for j := i + 1; j < len(buf); j++ {
				if buf[j] == ' ' {
					return string(buf[i+1 : j])
				}
			}
		}
	}
	return fmt.Sprintf("unknown-%d", time.Now().UnixNano())
}

// HealthChecker monitors goroutine health and detects issues
type HealthChecker struct {
	goroutines map[string]*GoroutineHealth
	mutex      sync.RWMutex
	interval   time.Duration
	stopChan   chan struct{}
}

// GoroutineHealth tracks health information for a goroutine
type GoroutineHealth struct {
	ID           string
	LastSeen     time.Time
	PanicCount   int
	RestartCount int
	IsHealthy    bool
	Metadata     map[string]interface{}
}

// NewHealthChecker creates a new health checker
func NewHealthChecker(checkInterval time.Duration) *HealthChecker {
	return &HealthChecker{
		goroutines: make(map[string]*GoroutineHealth),
		interval:   checkInterval,
		stopChan:   make(chan struct{}),
	}
}

// RegisterGoroutine registers a goroutine for health monitoring
func (hc *HealthChecker) RegisterGoroutine(id string, metadata map[string]interface{}) {
	hc.mutex.Lock()
	defer hc.mutex.Unlock()
	
	hc.goroutines[id] = &GoroutineHealth{
		ID:        id,
		LastSeen:  time.Now(),
		IsHealthy: true,
		Metadata:  metadata,
	}
}

// UpdateGoroutineHealth updates the health status of a goroutine
func (hc *HealthChecker) UpdateGoroutineHealth(id string, isHealthy bool) {
	hc.mutex.Lock()
	defer hc.mutex.Unlock()
	
	if health, exists := hc.goroutines[id]; exists {
		health.LastSeen = time.Now()
		health.IsHealthy = isHealthy
	}
}

// RecordPanic records a panic for a goroutine
func (hc *HealthChecker) RecordPanic(id string) {
	hc.mutex.Lock()
	defer hc.mutex.Unlock()
	
	if health, exists := hc.goroutines[id]; exists {
		health.PanicCount++
		health.IsHealthy = false
		health.LastSeen = time.Now()
	}
}

// RecordRestart records a restart for a goroutine
func (hc *HealthChecker) RecordRestart(id string) {
	hc.mutex.Lock()
	defer hc.mutex.Unlock()
	
	if health, exists := hc.goroutines[id]; exists {
		health.RestartCount++
		health.LastSeen = time.Now()
	}
}

// GetGoroutineHealth returns health information for a goroutine
func (hc *HealthChecker) GetGoroutineHealth(id string) (*GoroutineHealth, bool) {
	hc.mutex.RLock()
	defer hc.mutex.RUnlock()
	
	health, exists := hc.goroutines[id]
	if !exists {
		return nil, false
	}
	
	// Return a copy to prevent external modification
	return &GoroutineHealth{
		ID:           health.ID,
		LastSeen:     health.LastSeen,
		PanicCount:   health.PanicCount,
		RestartCount: health.RestartCount,
		IsHealthy:    health.IsHealthy,
		Metadata:     health.Metadata,
	}, true
}

// GetAllGoroutineHealth returns health information for all registered goroutines
func (hc *HealthChecker) GetAllGoroutineHealth() map[string]*GoroutineHealth {
	hc.mutex.RLock()
	defer hc.mutex.RUnlock()
	
	result := make(map[string]*GoroutineHealth)
	for id, health := range hc.goroutines {
		result[id] = &GoroutineHealth{
			ID:           health.ID,
			LastSeen:     health.LastSeen,
			PanicCount:   health.PanicCount,
			RestartCount: health.RestartCount,
			IsHealthy:    health.IsHealthy,
			Metadata:     health.Metadata,
		}
	}
	return result
}

// StartHealthMonitoring starts the health monitoring process
func (hc *HealthChecker) StartHealthMonitoring(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(hc.interval)
		defer ticker.Stop()
		
		for {
			select {
			case <-ticker.C:
				hc.checkGoroutineHealth()
			case <-hc.stopChan:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// StopHealthMonitoring stops the health monitoring process
func (hc *HealthChecker) StopHealthMonitoring() {
	close(hc.stopChan)
}

// checkGoroutineHealth checks the health of all registered goroutines
func (hc *HealthChecker) checkGoroutineHealth() {
	hc.mutex.Lock()
	defer hc.mutex.Unlock()
	
	now := time.Now()
	staleThreshold := 30 * time.Second // Consider goroutine stale if not seen for 30 seconds
	
	for id, health := range hc.goroutines {
		if now.Sub(health.LastSeen) > staleThreshold {
			if health.IsHealthy {
				log.Printf("Goroutine %s appears to be stale (last seen: %v)", id, health.LastSeen)
				health.IsHealthy = false
			}
		}
	}
}

// UnregisterGoroutine removes a goroutine from health monitoring
func (hc *HealthChecker) UnregisterGoroutine(id string) {
	hc.mutex.Lock()
	defer hc.mutex.Unlock()
	delete(hc.goroutines, id)
}