package redis

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

// CircuitBreakerState represents the state of the circuit breaker
type CircuitBreakerState int

const (
	CircuitBreakerClosed CircuitBreakerState = iota
	CircuitBreakerOpen
	CircuitBreakerHalfOpen
)

// CircuitBreaker implements the circuit breaker pattern for Redis operations
type CircuitBreaker struct {
	mutex           sync.RWMutex
	state           CircuitBreakerState
	failureCount    int
	successCount    int
	lastFailureTime time.Time
	timeout         time.Duration
	maxFailures     int
	resetTimeout    time.Duration
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(maxFailures int, timeout, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:        CircuitBreakerClosed,
		maxFailures:  maxFailures,
		timeout:      timeout,
		resetTimeout: resetTimeout,
	}
}

// Execute executes a function with circuit breaker protection
func (cb *CircuitBreaker) Execute(fn func() error) error {
	if !cb.canExecute() {
		return errors.New("circuit breaker is open")
	}

	err := fn()
	cb.recordResult(err)
	return err
}

// canExecute checks if the circuit breaker allows execution
func (cb *CircuitBreaker) canExecute() bool {
	cb.mutex.RLock()
	defer cb.mutex.RUnlock()

	switch cb.state {
	case CircuitBreakerClosed:
		return true
	case CircuitBreakerOpen:
		return time.Since(cb.lastFailureTime) >= cb.resetTimeout
	case CircuitBreakerHalfOpen:
		return true
	default:
		return false
	}
}

// recordResult records the result of an operation
func (cb *CircuitBreaker) recordResult(err error) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	if err != nil {
		cb.failureCount++
		cb.lastFailureTime = time.Now()

		switch cb.state {
		case CircuitBreakerClosed:
			if cb.failureCount >= cb.maxFailures {
				cb.state = CircuitBreakerOpen
				log.Printf("Circuit breaker opened due to %d failures", cb.failureCount)
			}
		case CircuitBreakerHalfOpen:
			cb.state = CircuitBreakerOpen
			log.Printf("Circuit breaker reopened after failure in half-open state")
		}
	} else {
		cb.successCount++

		switch cb.state {
		case CircuitBreakerHalfOpen:
			if cb.successCount >= 3 { // Require 3 successful operations to close
				cb.state = CircuitBreakerClosed
				cb.failureCount = 0
				cb.successCount = 0
				log.Printf("Circuit breaker closed after successful operations")
			}
		case CircuitBreakerOpen:
			if time.Since(cb.lastFailureTime) >= cb.resetTimeout {
				cb.state = CircuitBreakerHalfOpen
				cb.successCount = 1
				log.Printf("Circuit breaker moved to half-open state")
			}
		}
	}
}

// GetState returns the current state of the circuit breaker
func (cb *CircuitBreaker) GetState() CircuitBreakerState {
	cb.mutex.RLock()
	defer cb.mutex.RUnlock()
	return cb.state
}

// RetryConfig holds configuration for retry logic
type RetryConfig struct {
	MaxRetries      int
	BaseDelay       time.Duration
	MaxDelay        time.Duration
	BackoffFactor   float64
	JitterEnabled   bool
	RetryableErrors []error
}

// DefaultRetryConfig returns a default retry configuration
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxRetries:    3,
		BaseDelay:     100 * time.Millisecond,
		MaxDelay:      5 * time.Second,
		BackoffFactor: 2.0,
		JitterEnabled: true,
		RetryableErrors: []error{
			redis.TxFailedErr,
			context.DeadlineExceeded,
		},
	}
}

// ErrorHandler handles Redis errors with retry logic and circuit breaker
type ErrorHandler struct {
	circuitBreaker *CircuitBreaker
	retryConfig    *RetryConfig
}

// NewErrorHandler creates a new error handler
func NewErrorHandler(circuitBreaker *CircuitBreaker, retryConfig *RetryConfig) *ErrorHandler {
	if retryConfig == nil {
		retryConfig = DefaultRetryConfig()
	}
	
	return &ErrorHandler{
		circuitBreaker: circuitBreaker,
		retryConfig:    retryConfig,
	}
}

// ExecuteWithRetry executes a function with retry logic and circuit breaker protection
func (eh *ErrorHandler) ExecuteWithRetry(ctx context.Context, operation string, fn func() error) error {
	return eh.circuitBreaker.Execute(func() error {
		return eh.retryWithBackoff(ctx, operation, fn)
	})
}

// retryWithBackoff implements exponential backoff retry logic
func (eh *ErrorHandler) retryWithBackoff(ctx context.Context, operation string, fn func() error) error {
	var lastErr error
	
	for attempt := 0; attempt <= eh.retryConfig.MaxRetries; attempt++ {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Execute the function
		err := fn()
		if err == nil {
			if attempt > 0 {
				log.Printf("Redis operation '%s' succeeded after %d retries", operation, attempt)
			}
			return nil
		}

		lastErr = err

		// Check if error is retryable
		if !eh.isRetryableError(err) {
			log.Printf("Redis operation '%s' failed with non-retryable error: %v", operation, err)
			return err
		}

		// Don't retry on the last attempt
		if attempt == eh.retryConfig.MaxRetries {
			break
		}

		// Calculate delay with exponential backoff and jitter
		delay := eh.calculateDelay(attempt)
		log.Printf("Redis operation '%s' failed (attempt %d/%d), retrying in %v: %v", 
			operation, attempt+1, eh.retryConfig.MaxRetries+1, delay, err)

		// Wait before retry
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}

	return fmt.Errorf("Redis operation '%s' failed after %d retries: %w", 
		operation, eh.retryConfig.MaxRetries+1, lastErr)
}

// isRetryableError checks if an error is retryable
func (eh *ErrorHandler) isRetryableError(err error) bool {
	// Always retry on network errors, timeouts, and connection issues
	if isNetworkError(err) || isTimeoutError(err) || isConnectionError(err) {
		return true
	}

	// Check configured retryable errors
	for _, retryableErr := range eh.retryConfig.RetryableErrors {
		if errors.Is(err, retryableErr) {
			return true
		}
	}

	// Don't retry on data validation errors or client errors
	return false
}

// calculateDelay calculates the delay for the next retry with exponential backoff and jitter
func (eh *ErrorHandler) calculateDelay(attempt int) time.Duration {
	// Calculate exponential backoff
	delay := float64(eh.retryConfig.BaseDelay) * math.Pow(eh.retryConfig.BackoffFactor, float64(attempt))
	
	// Apply maximum delay limit
	if delay > float64(eh.retryConfig.MaxDelay) {
		delay = float64(eh.retryConfig.MaxDelay)
	}

	// Add jitter to prevent thundering herd
	if eh.retryConfig.JitterEnabled {
		jitter := rand.Float64() * 0.1 * delay // 10% jitter
		delay += jitter
	}

	return time.Duration(delay)
}

// isNetworkError checks if an error is a network-related error
func isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	
	errStr := err.Error()
	networkErrors := []string{
		"connection refused",
		"connection reset",
		"network is unreachable",
		"no route to host",
		"connection timeout",
		"i/o timeout",
		"broken pipe",
		"connection lost",
	}

	for _, netErr := range networkErrors {
		if contains(errStr, netErr) {
			return true
		}
	}

	return false
}

// isTimeoutError checks if an error is a timeout error
func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, context.DeadlineExceeded) || 
		   contains(err.Error(), "timeout") ||
		   contains(err.Error(), "deadline exceeded")
}

// isConnectionError checks if an error is a connection error
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	connectionErrors := []string{
		"connection closed",
		"connection refused",
		"connection reset",
		"connection lost",
		"connection aborted",
		"redis: client is closed",
		"redis: connection pool exhausted",
	}

	errStr := err.Error()
	for _, connErr := range connectionErrors {
		if contains(errStr, connErr) {
			return true
		}
	}

	return false
}

// contains checks if a string contains a substring (case-insensitive)
func contains(s, substr string) bool {
	return len(s) >= len(substr) && 
		   (s == substr || 
		    (len(s) > len(substr) && 
		     (s[:len(substr)] == substr || 
		      s[len(s)-len(substr):] == substr || 
		      containsSubstring(s, substr))))
}

// containsSubstring performs case-insensitive substring search
func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// FallbackManager manages fallback mechanisms when Redis is unavailable
type FallbackManager struct {
	inMemoryCache map[string]interface{}
	mutex         sync.RWMutex
	enabled       bool
}

// NewFallbackManager creates a new fallback manager
func NewFallbackManager() *FallbackManager {
	return &FallbackManager{
		inMemoryCache: make(map[string]interface{}),
		enabled:       true,
	}
}

// SetEnabled enables or disables fallback mode
func (fm *FallbackManager) SetEnabled(enabled bool) {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()
	fm.enabled = enabled
}

// IsEnabled returns whether fallback mode is enabled
func (fm *FallbackManager) IsEnabled() bool {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()
	return fm.enabled
}

// Store stores data in the fallback cache
func (fm *FallbackManager) Store(key string, value interface{}) {
	if !fm.IsEnabled() {
		return
	}

	fm.mutex.Lock()
	defer fm.mutex.Unlock()
	fm.inMemoryCache[key] = value
	log.Printf("Stored data in fallback cache for key: %s", key)
}

// Retrieve retrieves data from the fallback cache
func (fm *FallbackManager) Retrieve(key string) (interface{}, bool) {
	if !fm.IsEnabled() {
		return nil, false
	}

	fm.mutex.RLock()
	defer fm.mutex.RUnlock()
	value, exists := fm.inMemoryCache[key]
	return value, exists
}

// Clear clears the fallback cache
func (fm *FallbackManager) Clear() {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()
	fm.inMemoryCache = make(map[string]interface{})
	log.Printf("Cleared fallback cache")
}

// GetCacheSize returns the number of items in the fallback cache
func (fm *FallbackManager) GetCacheSize() int {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()
	return len(fm.inMemoryCache)
}