package performance

import (
	"context"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"aggregatorService/config"
	"aggregatorService/interfaces"
	"aggregatorService/logging"
)

// PerformanceTuner handles runtime performance optimization
type PerformanceTuner struct {
	cfg               *config.Config
	logger            *logging.Logger
	metricsCollector  interfaces.MetricsCollector
	
	// Performance monitoring
	lastGCStats       debug.GCStats
	lastMemStats      runtime.MemStats
	lastTuneTime      time.Time
	
	// Tuning state
	currentGCTarget   int
	currentMaxProcs   int
	
	mu                sync.RWMutex
}

// NewPerformanceTuner creates a new performance tuner
func NewPerformanceTuner(cfg *config.Config, logger *logging.Logger, metricsCollector interfaces.MetricsCollector) *PerformanceTuner {
	return &PerformanceTuner{
		cfg:              cfg,
		logger:           logger.WithOperation("performance_tuning"),
		metricsCollector: metricsCollector,
		lastTuneTime:     time.Now(),
		currentGCTarget:  cfg.GCTargetPercentage,
		currentMaxProcs:  runtime.GOMAXPROCS(0),
	}
}

// Initialize sets up initial performance optimizations
func (pt *PerformanceTuner) Initialize() error {
	pt.logger.Info("Initializing performance tuner")
	
	// Set initial GC target
	debug.SetGCPercent(pt.cfg.GCTargetPercentage)
	pt.logger.Info("Set GC target percentage", map[string]interface{}{
		"gc_target": pt.cfg.GCTargetPercentage,
	})
	
	// Set memory limit if specified
	if pt.cfg.MemoryLimitMB > 0 {
		memoryLimit := int64(pt.cfg.MemoryLimitMB) * 1024 * 1024
		debug.SetMemoryLimit(memoryLimit)
		pt.logger.Info("Set memory limit", map[string]interface{}{
			"memory_limit_mb": pt.cfg.MemoryLimitMB,
			"memory_limit_bytes": memoryLimit,
		})
	}
	
	// Log initial runtime settings
	pt.logger.Info("Initial runtime configuration", map[string]interface{}{
		"gomaxprocs":     runtime.GOMAXPROCS(0),
		"num_cpu":        runtime.NumCPU(),
		"gc_target":      pt.cfg.GCTargetPercentage,
		"memory_limit":   pt.cfg.MemoryLimitMB,
	})
	
	return nil
}

// TunePerformance performs runtime performance tuning based on current metrics
func (pt *PerformanceTuner) TunePerformance(ctx context.Context) error {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	
	now := time.Now()
	if now.Sub(pt.lastTuneTime) < 30*time.Second {
		return nil // Don't tune too frequently
	}
	
	pt.logger.Debug("Starting performance tuning cycle")
	
	// Collect current metrics
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	
	var gcStats debug.GCStats
	debug.ReadGCStats(&gcStats)
	
	// Get system metrics from collector
	metrics := pt.metricsCollector.GetMetrics()
	
	// Perform memory optimization
	if err := pt.optimizeMemory(&memStats, &gcStats); err != nil {
		pt.logger.Error("Memory optimization failed", map[string]interface{}{
			"error": err.Error(),
		})
	}
	
	// Perform GC optimization
	if err := pt.optimizeGC(&memStats, &gcStats); err != nil {
		pt.logger.Error("GC optimization failed", map[string]interface{}{
			"error": err.Error(),
		})
	}
	
	// Perform concurrency optimization
	if err := pt.optimizeConcurrency(metrics); err != nil {
		pt.logger.Error("Concurrency optimization failed", map[string]interface{}{
			"error": err.Error(),
		})
	}
	
	// Update last tune time and stats
	pt.lastTuneTime = now
	pt.lastMemStats = memStats
	pt.lastGCStats = gcStats
	
	pt.logger.Debug("Performance tuning cycle completed")
	return nil
}

// optimizeMemory optimizes memory usage based on current statistics
func (pt *PerformanceTuner) optimizeMemory(memStats *runtime.MemStats, gcStats *debug.GCStats) error {
	// Calculate memory usage metrics
	heapUsedMB := float64(memStats.HeapInuse) / 1024 / 1024
	heapAllocMB := float64(memStats.HeapAlloc) / 1024 / 1024
	memoryLimitMB := float64(pt.cfg.MemoryLimitMB)
	
	// Check if we're approaching memory limit
	if memoryLimitMB > 0 && heapUsedMB > memoryLimitMB*0.8 {
		pt.logger.Warn("Approaching memory limit, forcing GC", map[string]interface{}{
			"heap_used_mb":     heapUsedMB,
			"memory_limit_mb":  memoryLimitMB,
			"usage_percentage": (heapUsedMB / memoryLimitMB) * 100,
		})
		
		// Force garbage collection
		runtime.GC()
		
		// Read updated stats
		runtime.ReadMemStats(memStats)
		newHeapUsedMB := float64(memStats.HeapInuse) / 1024 / 1024
		
		pt.logger.Info("Forced GC completed", map[string]interface{}{
			"heap_used_before_mb": heapUsedMB,
			"heap_used_after_mb":  newHeapUsedMB,
			"memory_freed_mb":     heapUsedMB - newHeapUsedMB,
		})
	}
	
	// Log memory statistics
	pt.logger.Debug("Memory statistics", map[string]interface{}{
		"heap_alloc_mb":    heapAllocMB,
		"heap_inuse_mb":    heapUsedMB,
		"heap_sys_mb":      float64(memStats.HeapSys) / 1024 / 1024,
		"num_gc":           memStats.NumGC,
		"gc_cpu_fraction":  memStats.GCCPUFraction,
	})
	
	return nil
}

// optimizeGC optimizes garbage collection based on current performance
func (pt *PerformanceTuner) optimizeGC(memStats *runtime.MemStats, gcStats *debug.GCStats) error {
	// Calculate GC performance metrics
	gcCPUFraction := memStats.GCCPUFraction
	
	// Adjust GC target based on CPU usage
	newGCTarget := pt.currentGCTarget
	
	if gcCPUFraction > 0.05 { // GC using more than 5% CPU
		// Increase GC target to reduce frequency
		newGCTarget = min(pt.currentGCTarget+20, 500)
		pt.logger.Info("High GC CPU usage detected, increasing GC target", map[string]interface{}{
			"gc_cpu_fraction": gcCPUFraction,
			"old_gc_target":   pt.currentGCTarget,
			"new_gc_target":   newGCTarget,
		})
	} else if gcCPUFraction < 0.01 && pt.currentGCTarget > pt.cfg.GCTargetPercentage {
		// Decrease GC target to reclaim memory more aggressively
		newGCTarget = max(pt.currentGCTarget-10, pt.cfg.GCTargetPercentage)
		pt.logger.Info("Low GC CPU usage detected, decreasing GC target", map[string]interface{}{
			"gc_cpu_fraction": gcCPUFraction,
			"old_gc_target":   pt.currentGCTarget,
			"new_gc_target":   newGCTarget,
		})
	}
	
	// Apply GC target change if needed
	if newGCTarget != pt.currentGCTarget {
		debug.SetGCPercent(newGCTarget)
		pt.currentGCTarget = newGCTarget
	}
	
	return nil
}

// optimizeConcurrency optimizes concurrency settings based on system load
func (pt *PerformanceTuner) optimizeConcurrency(metrics map[string]interface{}) error {
	// Get current goroutine count
	numGoroutines := runtime.NumGoroutine()
	
	// Log concurrency statistics
	pt.logger.Debug("Concurrency statistics", map[string]interface{}{
		"num_goroutines": numGoroutines,
		"gomaxprocs":     runtime.GOMAXPROCS(0),
		"num_cpu":        runtime.NumCPU(),
	})
	
	// Check for goroutine leaks
	if numGoroutines > 10000 {
		pt.logger.Warn("High goroutine count detected, possible leak", map[string]interface{}{
			"num_goroutines": numGoroutines,
		})
	}
	
	return nil
}

// GetPerformanceStats returns current performance statistics
func (pt *PerformanceTuner) GetPerformanceStats() map[string]interface{} {
	pt.mu.RLock()
	defer pt.mu.RUnlock()
	
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	
	return map[string]interface{}{
		"memory": map[string]interface{}{
			"heap_alloc_mb":    float64(memStats.HeapAlloc) / 1024 / 1024,
			"heap_inuse_mb":    float64(memStats.HeapInuse) / 1024 / 1024,
			"heap_sys_mb":      float64(memStats.HeapSys) / 1024 / 1024,
			"num_gc":           memStats.NumGC,
			"gc_cpu_fraction":  memStats.GCCPUFraction,
		},
		"runtime": map[string]interface{}{
			"num_goroutines":   runtime.NumGoroutine(),
			"gomaxprocs":       runtime.GOMAXPROCS(0),
			"num_cpu":          runtime.NumCPU(),
			"gc_target":        pt.currentGCTarget,
		},
		"tuning": map[string]interface{}{
			"last_tune_time":   pt.lastTuneTime,
			"tune_interval":    time.Since(pt.lastTuneTime),
		},
	}
}

// ForceGC forces garbage collection and returns statistics
func (pt *PerformanceTuner) ForceGC() map[string]interface{} {
	pt.logger.Info("Forcing garbage collection")
	
	var beforeStats runtime.MemStats
	runtime.ReadMemStats(&beforeStats)
	
	start := time.Now()
	runtime.GC()
	duration := time.Since(start)
	
	var afterStats runtime.MemStats
	runtime.ReadMemStats(&afterStats)
	
	stats := map[string]interface{}{
		"gc_duration_ms":       duration.Milliseconds(),
		"heap_before_mb":       float64(beforeStats.HeapAlloc) / 1024 / 1024,
		"heap_after_mb":        float64(afterStats.HeapAlloc) / 1024 / 1024,
		"memory_freed_mb":      float64(beforeStats.HeapAlloc-afterStats.HeapAlloc) / 1024 / 1024,
		"gc_count_before":      beforeStats.NumGC,
		"gc_count_after":       afterStats.NumGC,
	}
	
	pt.logger.Info("Forced GC completed", stats)
	return stats
}

// OptimizeForHighThroughput optimizes settings for high-throughput scenarios
func (pt *PerformanceTuner) OptimizeForHighThroughput() error {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	
	pt.logger.Info("Optimizing for high throughput")
	
	// Increase GC target to reduce GC frequency
	newGCTarget := 200
	debug.SetGCPercent(newGCTarget)
	pt.currentGCTarget = newGCTarget
	
	pt.logger.Info("High throughput optimization applied", map[string]interface{}{
		"gc_target": newGCTarget,
	})
	
	return nil
}

// OptimizeForLowLatency optimizes settings for low-latency scenarios
func (pt *PerformanceTuner) OptimizeForLowLatency() error {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	
	pt.logger.Info("Optimizing for low latency")
	
	// Decrease GC target for more frequent cleanup
	newGCTarget := 50
	debug.SetGCPercent(newGCTarget)
	pt.currentGCTarget = newGCTarget
	
	pt.logger.Info("Low latency optimization applied", map[string]interface{}{
		"gc_target": newGCTarget,
	})
	
	return nil
}

// Helper functions
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}