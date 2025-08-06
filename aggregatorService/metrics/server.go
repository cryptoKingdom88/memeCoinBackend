package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"aggregatorService/interfaces"
)

// HealthChecker interface for checking service health
type HealthChecker interface {
	IsHealthy() bool
	IsStarted() bool
	GetUptime() time.Duration
}

// PerformanceTuner interface for performance operations
type PerformanceTuner interface {
	GetPerformanceStats() map[string]interface{}
	ForceGC() map[string]interface{}
	OptimizeForHighThroughput() error
	OptimizeForLowLatency() error
}

// Server provides HTTP endpoints for metrics
type Server struct {
	collector        interfaces.MetricsCollector
	healthChecker    HealthChecker
	performanceTuner PerformanceTuner
	server           *http.Server
	port             int
}

// NewServer creates a new metrics HTTP server
func NewServer(collector interfaces.MetricsCollector, port int) *Server {
	return &Server{
		collector: collector,
		port:      port,
	}
}

// NewServerWithHealthChecker creates a new metrics HTTP server with health checking
func NewServerWithHealthChecker(collector interfaces.MetricsCollector, healthChecker HealthChecker, port int) *Server {
	return &Server{
		collector:     collector,
		healthChecker: healthChecker,
		port:          port,
	}
}

// NewServerWithPerformanceTuner creates a new metrics HTTP server with performance tuning
func NewServerWithPerformanceTuner(collector interfaces.MetricsCollector, healthChecker HealthChecker, performanceTuner PerformanceTuner, port int) *Server {
	return &Server{
		collector:        collector,
		healthChecker:    healthChecker,
		performanceTuner: performanceTuner,
		port:             port,
	}
}

// Start starts the metrics HTTP server
func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	
	// Metrics endpoints
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/metrics/json", s.handleMetricsJSON)
	
	// Health check endpoints
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/health/live", s.handleLiveness)
	mux.HandleFunc("/health/ready", s.handleReadiness)
	
	// Performance endpoints
	if s.performanceTuner != nil {
		mux.HandleFunc("/performance", s.handlePerformance)
		mux.HandleFunc("/performance/gc", s.handleForceGC)
		mux.HandleFunc("/performance/optimize/throughput", s.handleOptimizeThroughput)
		mux.HandleFunc("/performance/optimize/latency", s.handleOptimizeLatency)
	}
	
	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	
	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Metrics server error: %v\n", err)
		}
	}()
	
	return nil
}

// Stop stops the metrics HTTP server
func (s *Server) Stop(ctx context.Context) error {
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

// handleMetrics returns metrics in Prometheus format
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := s.collector.GetMetrics()
	
	w.Header().Set("Content-Type", "text/plain")
	
	// Convert metrics to Prometheus format
	fmt.Fprintf(w, "# HELP realtime_aggregation_trades_processed_total Total number of trades processed\n")
	fmt.Fprintf(w, "# TYPE realtime_aggregation_trades_processed_total counter\n")
	if tradeMetrics, ok := metrics["trade_processing"].(*TradeMetrics); ok {
		fmt.Fprintf(w, "realtime_aggregation_trades_processed_total %d\n", tradeMetrics.TotalProcessed)
		fmt.Fprintf(w, "realtime_aggregation_trade_processing_rate %f\n", tradeMetrics.ProcessingRate)
		fmt.Fprintf(w, "realtime_aggregation_trade_processing_time_avg_ms %f\n", float64(tradeMetrics.AvgProcessingTime.Nanoseconds())/1e6)
	}
	
	fmt.Fprintf(w, "# HELP realtime_aggregation_calculations_total Total number of aggregation calculations\n")
	fmt.Fprintf(w, "# TYPE realtime_aggregation_calculations_total counter\n")
	if aggMetrics, ok := metrics["aggregation_calculation"].(*AggregationMetrics); ok {
		fmt.Fprintf(w, "realtime_aggregation_calculations_total %d\n", aggMetrics.TotalCalculations)
		fmt.Fprintf(w, "realtime_aggregation_calculation_rate %f\n", aggMetrics.CalculationRate)
		fmt.Fprintf(w, "realtime_aggregation_calculation_time_avg_ms %f\n", float64(aggMetrics.AvgLatency.Nanoseconds())/1e6)
	}
	
	fmt.Fprintf(w, "# HELP realtime_aggregation_redis_operations_total Total number of Redis operations\n")
	fmt.Fprintf(w, "# TYPE realtime_aggregation_redis_operations_total counter\n")
	if redisMetrics, ok := metrics["redis_operations"].(*RedisMetrics); ok {
		fmt.Fprintf(w, "realtime_aggregation_redis_operations_total %d\n", redisMetrics.TotalOperations)
		fmt.Fprintf(w, "realtime_aggregation_redis_success_rate %f\n", redisMetrics.SuccessRate)
		fmt.Fprintf(w, "realtime_aggregation_redis_latency_avg_ms %f\n", float64(redisMetrics.AvgLatency.Nanoseconds())/1e6)
	}
	
	fmt.Fprintf(w, "# HELP realtime_aggregation_active_tokens Current number of active tokens\n")
	fmt.Fprintf(w, "# TYPE realtime_aggregation_active_tokens gauge\n")
	if sysMetrics, ok := metrics["system_resources"].(*SystemMetrics); ok {
		fmt.Fprintf(w, "realtime_aggregation_active_tokens %d\n", sysMetrics.ActiveTokens)
		fmt.Fprintf(w, "realtime_aggregation_worker_count %d\n", sysMetrics.WorkerCount)
		fmt.Fprintf(w, "realtime_aggregation_goroutine_count %d\n", sysMetrics.GoroutineCount)
		fmt.Fprintf(w, "realtime_aggregation_memory_usage_bytes %d\n", sysMetrics.MemoryUsage)
		fmt.Fprintf(w, "realtime_aggregation_memory_allocated_bytes %d\n", sysMetrics.MemoryAllocated)
	}
	
	if uptime, ok := metrics["uptime_seconds"].(float64); ok {
		fmt.Fprintf(w, "realtime_aggregation_uptime_seconds %f\n", uptime)
	}
}

// handleMetricsJSON returns metrics in JSON format
func (s *Server) handleMetricsJSON(w http.ResponseWriter, r *http.Request) {
	metrics := s.collector.GetMetrics()
	
	w.Header().Set("Content-Type", "application/json")
	
	if err := json.NewEncoder(w).Encode(metrics); err != nil {
		http.Error(w, "Failed to encode metrics", http.StatusInternalServerError)
		return
	}
}

// handleHealth returns comprehensive health status
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	
	status := "healthy"
	statusCode := http.StatusOK
	
	if s.healthChecker != nil {
		if !s.healthChecker.IsHealthy() {
			status = "unhealthy"
			statusCode = http.StatusServiceUnavailable
		}
	}
	
	health := map[string]interface{}{
		"status":    status,
		"timestamp": time.Now().UTC(),
		"service":   "aggregator-service",
	}
	
	if s.healthChecker != nil {
		health["started"] = s.healthChecker.IsStarted()
		health["uptime_seconds"] = s.healthChecker.GetUptime().Seconds()
	}
	
	// Add metrics summary
	if metrics := s.collector.GetMetrics(); metrics != nil {
		health["metrics_summary"] = map[string]interface{}{
			"active_tokens": getMetricValue(metrics, "system_resources", "ActiveTokens"),
			"worker_count":  getMetricValue(metrics, "system_resources", "WorkerCount"),
		}
	}
	
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(health)
}

// handleLiveness returns liveness probe status (is the service running?)
func (s *Server) handleLiveness(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	
	// Service is live if the server is responding
	health := map[string]interface{}{
		"status":    "alive",
		"timestamp": time.Now().UTC(),
		"service":   "aggregator-service",
	}
	
	if s.healthChecker != nil {
		health["uptime_seconds"] = s.healthChecker.GetUptime().Seconds()
	}
	
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(health)
}

// handleReadiness returns readiness probe status (is the service ready to accept traffic?)
func (s *Server) handleReadiness(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	
	status := "ready"
	statusCode := http.StatusOK
	
	if s.healthChecker != nil {
		if !s.healthChecker.IsStarted() || !s.healthChecker.IsHealthy() {
			status = "not_ready"
			statusCode = http.StatusServiceUnavailable
		}
	}
	
	health := map[string]interface{}{
		"status":    status,
		"timestamp": time.Now().UTC(),
		"service":   "aggregator-service",
	}
	
	if s.healthChecker != nil {
		health["started"] = s.healthChecker.IsStarted()
		health["healthy"] = s.healthChecker.IsHealthy()
	}
	
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(health)
}

// getMetricValue safely extracts a metric value from the metrics map
func getMetricValue(metrics map[string]interface{}, category, field string) interface{} {
	if categoryData, ok := metrics[category]; ok {
		if categoryMap, ok := categoryData.(map[string]interface{}); ok {
			return categoryMap[field]
		}
		// Handle struct types by reflection
		if categoryStruct, ok := categoryData.(interface{}); ok {
			switch v := categoryStruct.(type) {
			case *SystemMetrics:
				switch field {
				case "ActiveTokens":
					return v.ActiveTokens
				case "WorkerCount":
					return v.WorkerCount
				case "GoroutineCount":
					return v.GoroutineCount
				case "MemoryUsage":
					return v.MemoryUsage
				}
			}
		}
	}
	return nil
}

// handlePerformance returns performance statistics
func (s *Server) handlePerformance(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	
	if s.performanceTuner == nil {
		http.Error(w, "Performance tuner not available", http.StatusServiceUnavailable)
		return
	}
	
	stats := s.performanceTuner.GetPerformanceStats()
	json.NewEncoder(w).Encode(stats)
}

// handleForceGC forces garbage collection
func (s *Server) handleForceGC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	
	if s.performanceTuner == nil {
		http.Error(w, "Performance tuner not available", http.StatusServiceUnavailable)
		return
	}
	
	stats := s.performanceTuner.ForceGC()
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "completed",
		"stats":  stats,
	})
}

// handleOptimizeThroughput optimizes for high throughput
func (s *Server) handleOptimizeThroughput(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	
	if s.performanceTuner == nil {
		http.Error(w, "Performance tuner not available", http.StatusServiceUnavailable)
		return
	}
	
	if err := s.performanceTuner.OptimizeForHighThroughput(); err != nil {
		http.Error(w, fmt.Sprintf("Optimization failed: %v", err), http.StatusInternalServerError)
		return
	}
	
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "completed",
		"message": "Optimized for high throughput",
	})
}

// handleOptimizeLatency optimizes for low latency
func (s *Server) handleOptimizeLatency(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	
	if s.performanceTuner == nil {
		http.Error(w, "Performance tuner not available", http.StatusServiceUnavailable)
		return
	}
	
	if err := s.performanceTuner.OptimizeForLowLatency(); err != nil {
		http.Error(w, fmt.Sprintf("Optimization failed: %v", err), http.StatusInternalServerError)
		return
	}
	
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "completed",
		"message": "Optimized for low latency",
	})
}