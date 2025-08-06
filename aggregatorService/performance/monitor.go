package performance

import (
	"context"
	"runtime"
	"sync"
	"time"

	"aggregatorService/interfaces"
	"aggregatorService/logging"
)

// Monitor continuously monitors system performance and triggers optimizations
type Monitor struct {
	tuner            *PerformanceTuner
	metricsCollector interfaces.MetricsCollector
	logger           *logging.Logger
	
	// Monitoring configuration
	monitorInterval  time.Duration
	tuneInterval     time.Duration
	
	// State
	running          bool
	stopChan         chan struct{}
	mu               sync.RWMutex
}

// NewMonitor creates a new performance monitor
func NewMonitor(tuner *PerformanceTuner, metricsCollector interfaces.MetricsCollector, logger *logging.Logger) *Monitor {
	return &Monitor{
		tuner:            tuner,
		metricsCollector: metricsCollector,
		logger:           logger.WithOperation("performance_monitor"),
		monitorInterval:  10 * time.Second,  // Monitor every 10 seconds
		tuneInterval:     60 * time.Second,  // Tune every 60 seconds
		stopChan:         make(chan struct{}),
	}
}

// Start starts the performance monitoring loop
func (m *Monitor) Start(ctx context.Context) error {
	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		return nil
	}
	m.running = true
	m.mu.Unlock()
	
	m.logger.Info("Starting performance monitor", map[string]interface{}{
		"monitor_interval": m.monitorInterval.String(),
		"tune_interval":    m.tuneInterval.String(),
	})
	
	go m.monitorLoop(ctx)
	return nil
}

// Stop stops the performance monitoring
func (m *Monitor) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if !m.running {
		return nil
	}
	
	m.logger.Info("Stopping performance monitor")
	m.running = false
	close(m.stopChan)
	
	return nil
}

// monitorLoop is the main monitoring loop
func (m *Monitor) monitorLoop(ctx context.Context) {
	monitorTicker := time.NewTicker(m.monitorInterval)
	tuneTicker := time.NewTicker(m.tuneInterval)
	
	defer func() {
		monitorTicker.Stop()
		tuneTicker.Stop()
		m.logger.Info("Performance monitor stopped")
	}()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopChan:
			return
		case <-monitorTicker.C:
			m.collectMetrics()
		case <-tuneTicker.C:
			if err := m.tuner.TunePerformance(ctx); err != nil {
				m.logger.Error("Performance tuning failed", map[string]interface{}{
					"error": err.Error(),
				})
			}
		}
	}
}

// collectMetrics collects and records system performance metrics
func (m *Monitor) collectMetrics() {
	// Collect memory statistics
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	
	// Record memory metrics
	m.metricsCollector.RecordMemoryUsage(int64(memStats.HeapInuse))
	
	// Collect goroutine count
	numGoroutines := runtime.NumGoroutine()
	
	// Log performance metrics periodically
	m.logger.Debug("Performance metrics collected", map[string]interface{}{
		"heap_alloc_mb":    float64(memStats.HeapAlloc) / 1024 / 1024,
		"heap_inuse_mb":    float64(memStats.HeapInuse) / 1024 / 1024,
		"num_goroutines":   numGoroutines,
		"num_gc":           memStats.NumGC,
		"gc_cpu_fraction":  memStats.GCCPUFraction,
	})
	
	// Check for performance issues
	m.checkPerformanceIssues(&memStats, numGoroutines)
}

// checkPerformanceIssues checks for common performance issues
func (m *Monitor) checkPerformanceIssues(memStats *runtime.MemStats, numGoroutines int) {
	// Check for high GC CPU usage
	if memStats.GCCPUFraction > 0.1 {
		m.logger.Warn("High GC CPU usage detected", map[string]interface{}{
			"gc_cpu_fraction": memStats.GCCPUFraction,
			"recommendation": "Consider increasing GC target percentage",
		})
	}
	
	// Check for potential goroutine leaks
	if numGoroutines > 5000 {
		m.logger.Warn("High goroutine count detected", map[string]interface{}{
			"num_goroutines": numGoroutines,
			"recommendation": "Check for goroutine leaks",
		})
	}
	
	// Check for high memory usage
	heapUsedMB := float64(memStats.HeapInuse) / 1024 / 1024
	if heapUsedMB > 400 { // Warn if using more than 400MB
		m.logger.Warn("High memory usage detected", map[string]interface{}{
			"heap_used_mb":   heapUsedMB,
			"recommendation": "Consider forcing GC or checking for memory leaks",
		})
	}
}

// GetMonitoringStats returns current monitoring statistics
func (m *Monitor) GetMonitoringStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	return map[string]interface{}{
		"running":          m.running,
		"monitor_interval": m.monitorInterval.String(),
		"tune_interval":    m.tuneInterval.String(),
	}
}

// IsRunning returns whether the monitor is currently running
func (m *Monitor) IsRunning() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.running
}