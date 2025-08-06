package metrics

import (
	"testing"
	"time"
)

func TestNewCollector(t *testing.T) {
	collector := NewCollector()
	if collector == nil {
		t.Fatal("NewCollector returned nil")
	}
	
	// Test initialization
	err := collector.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
}

func TestRecordTradeProcessed(t *testing.T) {
	collector := NewCollector().(*Collector)
	err := collector.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	
	// Record some trades
	collector.RecordTradeProcessed("token1", 10*time.Millisecond)
	collector.RecordTradeProcessed("token2", 20*time.Millisecond)
	collector.RecordTradeProcessed("token1", 15*time.Millisecond)
	
	metrics := collector.GetMetrics()
	tradeMetrics, ok := metrics["trade_processing"].(*TradeMetrics)
	if !ok {
		t.Fatal("Trade metrics not found")
	}
	
	if tradeMetrics.TotalProcessed != 3 {
		t.Errorf("Expected 3 trades processed, got %d", tradeMetrics.TotalProcessed)
	}
	
	if tradeMetrics.ProcessingRate <= 0 {
		t.Errorf("Expected positive processing rate, got %f", tradeMetrics.ProcessingRate)
	}
	
	expectedAvg := 15 * time.Millisecond // (10+20+15)/3
	if tradeMetrics.AvgProcessingTime != expectedAvg {
		t.Errorf("Expected avg processing time %v, got %v", expectedAvg, tradeMetrics.AvgProcessingTime)
	}
}

func TestRecordAggregationCalculated(t *testing.T) {
	collector := NewCollector().(*Collector)
	err := collector.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	
	// Record some calculations
	collector.RecordAggregationCalculated("token1", "1min", 5*time.Millisecond)
	collector.RecordAggregationCalculated("token1", "5min", 8*time.Millisecond)
	collector.RecordAggregationCalculated("token2", "1min", 12*time.Millisecond)
	
	metrics := collector.GetMetrics()
	aggMetrics, ok := metrics["aggregation_calculation"].(*AggregationMetrics)
	if !ok {
		t.Fatal("Aggregation metrics not found")
	}
	
	if aggMetrics.TotalCalculations != 3 {
		t.Errorf("Expected 3 calculations, got %d", aggMetrics.TotalCalculations)
	}
	
	if aggMetrics.CalculationRate <= 0 {
		t.Errorf("Expected positive calculation rate, got %f", aggMetrics.CalculationRate)
	}
	
	// Check latency by window
	if aggMetrics.LatencyByWindow["1min"] != 12*time.Millisecond {
		t.Errorf("Expected 1min latency 12ms, got %v", aggMetrics.LatencyByWindow["1min"])
	}
	
	if aggMetrics.LatencyByWindow["5min"] != 8*time.Millisecond {
		t.Errorf("Expected 5min latency 8ms, got %v", aggMetrics.LatencyByWindow["5min"])
	}
}

func TestRecordRedisOperation(t *testing.T) {
	collector := NewCollector().(*Collector)
	err := collector.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	
	// Record successful operations
	collector.RecordRedisOperation("GET", 2*time.Millisecond, true)
	collector.RecordRedisOperation("SET", 3*time.Millisecond, true)
	collector.RecordRedisOperation("GET", 4*time.Millisecond, false) // Failed operation
	
	metrics := collector.GetMetrics()
	redisMetrics, ok := metrics["redis_operations"].(*RedisMetrics)
	if !ok {
		t.Fatal("Redis metrics not found")
	}
	
	if redisMetrics.TotalOperations != 3 {
		t.Errorf("Expected 3 operations, got %d", redisMetrics.TotalOperations)
	}
	
	if redisMetrics.SuccessfulOps != 2 {
		t.Errorf("Expected 2 successful operations, got %d", redisMetrics.SuccessfulOps)
	}
	
	if redisMetrics.FailedOps != 1 {
		t.Errorf("Expected 1 failed operation, got %d", redisMetrics.FailedOps)
	}
	
	expectedSuccessRate := float64(2) / float64(3) * 100 // 66.67%
	if redisMetrics.SuccessRate != expectedSuccessRate {
		t.Errorf("Expected success rate %f, got %f", expectedSuccessRate, redisMetrics.SuccessRate)
	}
	
	// Check latency by operation (should be the last recorded value)
	if redisMetrics.LatencyByOp["GET"] != 4*time.Millisecond {
		t.Errorf("Expected GET latency 4ms, got %v", redisMetrics.LatencyByOp["GET"])
	}
	
	if redisMetrics.LatencyByOp["SET"] != 3*time.Millisecond {
		t.Errorf("Expected SET latency 3ms, got %v", redisMetrics.LatencyByOp["SET"])
	}
}

func TestRecordSystemMetrics(t *testing.T) {
	collector := NewCollector().(*Collector)
	err := collector.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	
	// Record system metrics
	collector.RecordActiveTokens(100)
	collector.RecordWorkerCount(50)
	collector.RecordMemoryUsage(1024*1024) // 1MB
	
	metrics := collector.GetMetrics()
	sysMetrics, ok := metrics["system_resources"].(*SystemMetrics)
	if !ok {
		t.Fatal("System metrics not found")
	}
	
	if sysMetrics.ActiveTokens != 100 {
		t.Errorf("Expected 100 active tokens, got %d", sysMetrics.ActiveTokens)
	}
	
	if sysMetrics.WorkerCount != 50 {
		t.Errorf("Expected 50 workers, got %d", sysMetrics.WorkerCount)
	}
	
	if sysMetrics.MemoryUsage != 1024*1024 {
		t.Errorf("Expected 1MB memory usage, got %d", sysMetrics.MemoryUsage)
	}
	
	// System metrics should include runtime data
	if sysMetrics.GoroutineCount <= 0 {
		t.Errorf("Expected positive goroutine count, got %d", sysMetrics.GoroutineCount)
	}
	
	if sysMetrics.MemoryAllocated <= 0 {
		t.Errorf("Expected positive memory allocated, got %d", sysMetrics.MemoryAllocated)
	}
}

func TestCalculateAverage(t *testing.T) {
	collector := &Collector{}
	
	// Test empty slice
	avg := collector.calculateAverage([]time.Duration{})
	if avg != 0 {
		t.Errorf("Expected 0 for empty slice, got %v", avg)
	}
	
	// Test single value
	durations := []time.Duration{10 * time.Millisecond}
	avg = collector.calculateAverage(durations)
	if avg != 10*time.Millisecond {
		t.Errorf("Expected 10ms, got %v", avg)
	}
	
	// Test multiple values
	durations = []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond}
	avg = collector.calculateAverage(durations)
	expected := 20 * time.Millisecond // (10+20+30)/3
	if avg != expected {
		t.Errorf("Expected %v, got %v", expected, avg)
	}
}

func TestMetricsIntegration(t *testing.T) {
	collector := NewCollector()
	err := collector.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	
	wrapper := NewMetricsWrapper(collector)
	
	// Test timed trade processing
	err = wrapper.TimedTradeProcessing("token1", func() error {
		time.Sleep(10 * time.Millisecond)
		return nil
	})
	if err != nil {
		t.Errorf("TimedTradeProcessing failed: %v", err)
	}
	
	// Test timed aggregation calculation
	err = wrapper.TimedAggregationCalculation("token1", "1min", func() error {
		time.Sleep(5 * time.Millisecond)
		return nil
	})
	if err != nil {
		t.Errorf("TimedAggregationCalculation failed: %v", err)
	}
	
	// Test timed Redis operation
	err = wrapper.TimedRedisOperation("GET", func() error {
		time.Sleep(2 * time.Millisecond)
		return nil
	})
	if err != nil {
		t.Errorf("TimedRedisOperation failed: %v", err)
	}
	
	// Verify metrics were recorded
	metrics := collector.GetMetrics()
	
	tradeMetrics, ok := metrics["trade_processing"].(*TradeMetrics)
	if !ok || tradeMetrics.TotalProcessed != 1 {
		t.Error("Trade processing metrics not recorded correctly")
	}
	
	aggMetrics, ok := metrics["aggregation_calculation"].(*AggregationMetrics)
	if !ok || aggMetrics.TotalCalculations != 1 {
		t.Error("Aggregation calculation metrics not recorded correctly")
	}
	
	redisMetrics, ok := metrics["redis_operations"].(*RedisMetrics)
	if !ok || redisMetrics.TotalOperations != 1 {
		t.Error("Redis operation metrics not recorded correctly")
	}
}