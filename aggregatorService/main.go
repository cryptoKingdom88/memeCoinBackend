package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"aggregatorService/aggregation"
	"aggregatorService/config"
	"aggregatorService/interfaces"
	"aggregatorService/kafka"
	"aggregatorService/logging"
	"aggregatorService/maintenance"
	"aggregatorService/metrics"
	"aggregatorService/performance"
	"aggregatorService/processor"
	"aggregatorService/redis"
	"aggregatorService/worker"
)

// Service represents the main service with all components
type Service struct {
	cfg                *config.Config
	logger             *logging.Logger
	redisManager       interfaces.RedisManager
	calculator         interfaces.SlidingWindowCalculator
	workerPool         interfaces.WorkerPool
	blockAggregator    interfaces.BlockAggregator
	kafkaConsumer      interfaces.KafkaConsumer
	kafkaProducer      interfaces.KafkaProducer
	maintenanceService interfaces.MaintenanceService
	metricsCollector   interfaces.MetricsCollector
	metricsServer      *metrics.Server
	performanceTuner   *performance.PerformanceTuner
	performanceMonitor *performance.Monitor
	
	// Service state
	started    bool
	healthy    bool
	startTime  time.Time
	mu         sync.RWMutex
}

func main() {
	// Create main service instance
	service := &Service{
		startTime: time.Now(),
	}
	
	// Initialize logger first
	service.logger = logging.NewLogger("aggregator-service", "main")
	service.logger.Info("Starting Aggregator Service")
	
	// Load configuration
	cfg := config.LoadConfig()
	service.cfg = cfg
	service.logger.Info("Configuration loaded", map[string]interface{}{
		"kafka_topic":    cfg.KafkaTopic,
		"time_windows":   len(cfg.TimeWindows),
		"max_workers":    cfg.MaxWorkers,
	})
	
	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize all components
	if err := service.initializeComponents(ctx); err != nil {
		service.logger.Fatal("Failed to initialize components", map[string]interface{}{
			"error": err.Error(),
		})
	}

	// Perform health checks
	if err := service.performHealthChecks(ctx); err != nil {
		service.logger.Fatal("Health checks failed", map[string]interface{}{
			"error": err.Error(),
		})
	}

	// Start all services
	if err := service.startServices(ctx); err != nil {
		service.logger.Fatal("Failed to start services", map[string]interface{}{
			"error": err.Error(),
		})
	}

	// Mark service as started and healthy
	service.mu.Lock()
	service.started = true
	service.healthy = true
	service.mu.Unlock()

	service.logger.Info("Aggregator Service started successfully", map[string]interface{}{
		"startup_duration": time.Since(service.startTime).String(),
	})

	// Wait for shutdown signal
	service.waitForShutdown(ctx, cancel)

	// Graceful shutdown
	service.logger.Info("Initiating graceful shutdown")
	shutdownStart := time.Now()
	
	service.mu.Lock()
	service.healthy = false
	service.mu.Unlock()
	
	service.gracefulShutdown(ctx)
	
	service.logger.Info("Service shutdown complete", map[string]interface{}{
		"shutdown_duration": time.Since(shutdownStart).String(),
		"total_uptime":      time.Since(service.startTime).String(),
	})
}

// initializeComponents initializes all service components
func (s *Service) initializeComponents(ctx context.Context) error {
	// Initialize Redis manager
	s.redisManager = redis.NewManager(s.cfg.RedisAddr, s.cfg.RedisPassword, s.cfg.RedisDB, s.cfg.RedisPoolSize)
	if err := s.redisManager.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to Redis: %w", err)
	}

	// Initialize sliding window calculator
	s.calculator = aggregation.NewCalculator()
	if err := s.calculator.Initialize(s.cfg.TimeWindows); err != nil {
		return fmt.Errorf("failed to initialize calculator: %w", err)
	}

	// Initialize worker pool
	s.workerPool = worker.NewPool()
	if err := s.workerPool.Initialize(s.cfg.MaxWorkers); err != nil {
		return fmt.Errorf("failed to initialize worker pool: %w", err)
	}

	// Initialize Kafka producer
	s.kafkaProducer = kafka.NewProducer()
	if err := s.kafkaProducer.Initialize(s.cfg.KafkaBrokers); err != nil {
		return fmt.Errorf("failed to initialize Kafka producer: %w", err)
	}

	// Initialize block aggregator
	s.blockAggregator = processor.NewBlockAggregator()
	if err := s.blockAggregator.Initialize(s.redisManager, s.calculator, s.workerPool, s.kafkaProducer); err != nil {
		return fmt.Errorf("failed to initialize block aggregator: %w", err)
	}

	// Initialize Kafka consumer
	s.kafkaConsumer = kafka.NewConsumer()
	if err := s.kafkaConsumer.Initialize(s.cfg.KafkaBrokers, s.cfg.KafkaTopic, s.cfg.ConsumerGroup); err != nil {
		return fmt.Errorf("failed to initialize Kafka consumer: %w", err)
	}

	// Initialize maintenance service
	s.maintenanceService = maintenance.NewService()
	if err := s.maintenanceService.Initialize(s.redisManager, s.cfg.MaintenanceInterval, s.cfg.StaleThreshold); err != nil {
		return fmt.Errorf("failed to initialize maintenance service: %w", err)
	}

	// Initialize metrics collector
	s.metricsCollector = metrics.NewCollector()
	if err := s.metricsCollector.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize metrics collector: %w", err)
	}

	// Initialize performance tuner
	s.performanceTuner = performance.NewPerformanceTuner(s.cfg, s.logger, s.metricsCollector)
	if err := s.performanceTuner.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize performance tuner: %w", err)
	}

	// Initialize metrics server if enabled
	if s.cfg.MetricsEnabled {
		s.metricsServer = metrics.NewServerWithPerformanceTuner(s.metricsCollector, s, s.performanceTuner, s.cfg.MetricsPort)
	}

	// Initialize performance monitor
	s.performanceMonitor = performance.NewMonitor(s.performanceTuner, s.metricsCollector, s.logger)

	s.logger.Info("Components initialized")
	return nil
}

// performHealthChecks performs initial health checks on all components
func (s *Service) performHealthChecks(ctx context.Context) error {
	// Check Redis connectivity
	if err := s.redisManager.Ping(ctx); err != nil {
		return fmt.Errorf("Redis health check failed: %w", err)
	}
	
	// Additional health checks can be added here for other components
	
	return nil
}

// startServices starts all background services
func (s *Service) startServices(ctx context.Context) error {
	var wg sync.WaitGroup
	errChan := make(chan error, 4) // Buffer for potential errors from 4 services
	
	// Start metrics server if enabled
	if s.cfg.MetricsEnabled && s.metricsServer != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.metricsServer.Start(ctx); err != nil {
				s.logger.Error("Metrics server failed to start", map[string]interface{}{
					"error": err.Error(),
				})
				errChan <- fmt.Errorf("metrics server failed: %w", err)
			}
		}()
	}
	
	// Start Kafka consumer
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.kafkaConsumer.Start(ctx, s.blockAggregator); err != nil {
			s.logger.Error("Kafka consumer failed", map[string]interface{}{
				"error": err.Error(),
			})
			errChan <- fmt.Errorf("Kafka consumer failed: %w", err)
		}
	}()

	// Start maintenance service
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.maintenanceService.Start(ctx); err != nil {
			s.logger.Error("Maintenance service failed", map[string]interface{}{
				"error": err.Error(),
			})
			errChan <- fmt.Errorf("maintenance service failed: %w", err)
		}
	}()

	// Start performance monitor
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.performanceMonitor.Start(ctx); err != nil {
			s.logger.Error("Performance monitor failed", map[string]interface{}{
				"error": err.Error(),
			})
			errChan <- fmt.Errorf("performance monitor failed: %w", err)
		}
	}()

	// Start periodic aggregate publishing (every 30 seconds)
	s.blockAggregator.SchedulePeriodicAggregatePublishing(ctx, 30*time.Second)
	
	// Wait a moment for services to start up
	time.Sleep(100 * time.Millisecond)
	
	// Check for immediate startup errors
	select {
	case err := <-errChan:
		return err
	default:
		// No immediate errors
	}
	
	return nil
}

// waitForShutdown waits for shutdown signals
func (s *Service) waitForShutdown(ctx context.Context, cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	
	select {
	case sig := <-sigChan:
		s.logger.Info("Received shutdown signal", map[string]interface{}{
			"signal": sig.String(),
		})
		cancel()
	case <-ctx.Done():
		s.logger.Info("Context cancelled, initiating shutdown")
	}
}

// gracefulShutdown performs graceful shutdown of all services
func (s *Service) gracefulShutdown(ctx context.Context) {
	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	
	var wg sync.WaitGroup
	
	// Stop metrics server first (stop accepting new requests)
	if s.metricsServer != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.logger.Info("Stopping metrics server")
			if err := s.metricsServer.Stop(shutdownCtx); err != nil {
				s.logger.Error("Error stopping metrics server", map[string]interface{}{
					"error": err.Error(),
				})
			} else {
				s.logger.Info("Metrics server stopped successfully")
			}
		}()
	}
	
	// Stop Kafka consumer (stop processing new messages)
	if s.kafkaConsumer != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.logger.Info("Stopping Kafka consumer")
			if err := s.kafkaConsumer.Stop(); err != nil {
				s.logger.Error("Error stopping Kafka consumer", map[string]interface{}{
					"error": err.Error(),
				})
			} else {
				s.logger.Info("Kafka consumer stopped successfully")
			}
		}()
	}

	// Stop maintenance service
	if s.maintenanceService != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.logger.Info("Stopping maintenance service")
			if err := s.maintenanceService.Stop(); err != nil {
				s.logger.Error("Error stopping maintenance service", map[string]interface{}{
					"error": err.Error(),
				})
			} else {
				s.logger.Info("Maintenance service stopped successfully")
			}
		}()
	}

	// Stop performance monitor
	if s.performanceMonitor != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.logger.Info("Stopping performance monitor")
			if err := s.performanceMonitor.Stop(); err != nil {
				s.logger.Error("Error stopping performance monitor", map[string]interface{}{
					"error": err.Error(),
				})
			} else {
				s.logger.Info("Performance monitor stopped successfully")
			}
		}()
	}
	
	// Wait for services to stop
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	
	select {
	case <-done:
		s.logger.Info("All services stopped successfully")
	case <-shutdownCtx.Done():
		s.logger.Warn("Shutdown timeout reached, forcing shutdown")
	}

	// Shutdown block aggregator (finish processing current trades)
	if s.blockAggregator != nil {
		s.logger.Info("Shutting down block aggregator")
		if err := s.blockAggregator.Shutdown(shutdownCtx); err != nil {
			s.logger.Error("Error shutting down block aggregator", map[string]interface{}{
				"error": err.Error(),
			})
		} else {
			s.logger.Info("Block aggregator shutdown successfully")
		}
	}

	// Shutdown worker pool (finish current jobs)
	if s.workerPool != nil {
		s.logger.Info("Shutting down worker pool")
		if err := s.workerPool.Shutdown(shutdownCtx); err != nil {
			s.logger.Error("Error shutting down worker pool", map[string]interface{}{
				"error": err.Error(),
			})
		} else {
			s.logger.Info("Worker pool shutdown successfully")
		}
	}

	// Close Kafka producer
	if s.kafkaProducer != nil {
		s.logger.Info("Closing Kafka producer")
		if err := s.kafkaProducer.Close(); err != nil {
			s.logger.Error("Error closing Kafka producer", map[string]interface{}{
				"error": err.Error(),
			})
		} else {
			s.logger.Info("Kafka producer closed successfully")
		}
	}

	// Close Redis connection (last to ensure all data is saved)
	if s.redisManager != nil {
		s.logger.Info("Closing Redis connection")
		if err := s.redisManager.Close(); err != nil {
			s.logger.Error("Error closing Redis connection", map[string]interface{}{
				"error": err.Error(),
			})
		} else {
			s.logger.Info("Redis connection closed successfully")
		}
	}
}

// IsHealthy returns the current health status of the service
func (s *Service) IsHealthy() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.healthy
}

// IsStarted returns whether the service has been started
func (s *Service) IsStarted() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.started
}

// GetUptime returns the service uptime
func (s *Service) GetUptime() time.Duration {
	return time.Since(s.startTime)
}