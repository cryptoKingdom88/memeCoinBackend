package worker

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"aggregatorService/interfaces"
)

// Job represents a work item to be processed
type Job struct {
	ID       uint64
	Function func()
	Created  time.Time
}

// Pool implements a worker pool to limit concurrent goroutines
type Pool struct {
	// Configuration
	maxWorkers int
	
	// Job management
	jobQueue    chan Job
	jobCounter  uint64
	
	// Worker management
	workers     []*Worker
	workerCount int32
	
	// Lifecycle management
	isInitialized bool
	isShutdown    bool
	shutdownChan  chan struct{}
	wg            sync.WaitGroup
	mutex         sync.RWMutex
	
	// Statistics
	stats *PoolStats
}

// Worker represents a single worker goroutine
type Worker struct {
	id       int
	pool     *Pool
	jobChan  chan Job
	quitChan chan struct{}
	wg       *sync.WaitGroup
}

// PoolStats tracks pool statistics
type PoolStats struct {
	JobsSubmitted   uint64
	JobsCompleted   uint64
	JobsInProgress  int32
	TotalWorkers    int32
	ActiveWorkers   int32
	QueueLength     int32
	mutex           sync.RWMutex
}

// NewPool creates a new worker pool
func NewPool() interfaces.WorkerPool {
	return &Pool{
		stats: &PoolStats{},
	}
}

// Initialize initializes the worker pool with the specified number of workers
func (p *Pool) Initialize(maxWorkers int) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if p.isInitialized {
		return fmt.Errorf("worker pool already initialized")
	}
	
	if maxWorkers <= 0 {
		return fmt.Errorf("maxWorkers must be positive, got: %d", maxWorkers)
	}
	
	p.maxWorkers = maxWorkers
	p.jobQueue = make(chan Job, maxWorkers*2) // Buffer 2x the number of workers
	p.shutdownChan = make(chan struct{})
	p.workers = make([]*Worker, maxWorkers)
	
	// Start workers
	for i := 0; i < maxWorkers; i++ {
		worker := &Worker{
			id:       i,
			pool:     p,
			jobChan:  make(chan Job),
			quitChan: make(chan struct{}),
			wg:       &p.wg,
		}
		
		p.workers[i] = worker
		p.wg.Add(1)
		go worker.start()
		atomic.AddInt32(&p.workerCount, 1)
		atomic.AddInt32(&p.stats.TotalWorkers, 1)
	}
	
	// Start job dispatcher
	p.wg.Add(1)
	go p.dispatcher()
	
	p.isInitialized = true
	log.Printf("WorkerPool initialized with %d workers", maxWorkers)
	
	return nil
}

// Submit submits a job to the worker pool
func (p *Pool) Submit(job func()) error {
	p.mutex.RLock()
	if p.isShutdown {
		p.mutex.RUnlock()
		return fmt.Errorf("worker pool is shut down")
	}
	if !p.isInitialized {
		p.mutex.RUnlock()
		return fmt.Errorf("worker pool not initialized")
	}
	p.mutex.RUnlock()
	
	if job == nil {
		return fmt.Errorf("job function cannot be nil")
	}
	
	// Create job with unique ID
	jobID := atomic.AddUint64(&p.jobCounter, 1)
	workJob := Job{
		ID:       jobID,
		Function: job,
		Created:  time.Now(),
	}
	
	// Submit job to queue (non-blocking)
	select {
	case p.jobQueue <- workJob:
		atomic.AddUint64(&p.stats.JobsSubmitted, 1)
		atomic.AddInt32(&p.stats.QueueLength, 1)
		return nil
	default:
		// Queue is full, return error
		return fmt.Errorf("job queue is full (capacity: %d)", cap(p.jobQueue))
	}
}

// GetWorkerCount returns the current number of workers
func (p *Pool) GetWorkerCount() int {
	return int(atomic.LoadInt32(&p.workerCount))
}

// Shutdown gracefully shuts down the worker pool
func (p *Pool) Shutdown(ctx context.Context) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if p.isShutdown {
		return nil // Already shut down
	}
	
	if !p.isInitialized {
		return fmt.Errorf("worker pool not initialized")
	}
	
	log.Printf("Shutting down WorkerPool with %d workers", p.maxWorkers)
	
	// Signal shutdown
	close(p.shutdownChan)
	
	// Close job queue to prevent new jobs
	close(p.jobQueue)
	
	// Wait for all workers to finish with timeout
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	
	select {
	case <-done:
		// Graceful shutdown completed
		log.Printf("WorkerPool shutdown completed gracefully")
	case <-ctx.Done():
		// Context timeout
		log.Printf("Warning: WorkerPool shutdown timed out due to context cancellation")
	case <-time.After(10 * time.Second):
		// Fallback timeout
		log.Printf("Warning: WorkerPool shutdown timed out after 10 seconds")
	}
	
	p.isShutdown = true
	return nil
}

// dispatcher distributes jobs to available workers
func (p *Pool) dispatcher() {
	defer p.wg.Done()
	
	for {
		select {
		case job, ok := <-p.jobQueue:
			if !ok {
				// Job queue closed, shutdown dispatcher
				return
			}
			
			atomic.AddInt32(&p.stats.QueueLength, -1)
			
			// Find an available worker
			p.dispatchJob(job)
			
		case <-p.shutdownChan:
			// Process remaining jobs in queue
			for {
				select {
				case job, ok := <-p.jobQueue:
					if !ok {
						return
					}
					atomic.AddInt32(&p.stats.QueueLength, -1)
					p.dispatchJob(job)
				default:
					return
				}
			}
		}
	}
}

// dispatchJob dispatches a job to an available worker
func (p *Pool) dispatchJob(job Job) {
	// Try to dispatch to any available worker
	for _, worker := range p.workers {
		select {
		case worker.jobChan <- job:
			return // Job dispatched successfully
		default:
			// Worker is busy, try next one
			continue
		}
	}
	
	// All workers are busy, wait for the first available one
	// This is a blocking operation but ensures job gets processed
	for _, worker := range p.workers {
		select {
		case worker.jobChan <- job:
			return
		case <-p.shutdownChan:
			// Shutdown requested, drop the job
			log.Printf("Warning: dropping job %d due to shutdown", job.ID)
			return
		}
	}
}

// start starts the worker goroutine
func (w *Worker) start() {
	defer w.wg.Done()
	
	log.Printf("Worker %d started", w.id)
	
	for {
		select {
		case job := <-w.jobChan:
			w.processJob(job)
			
		case <-w.quitChan:
			// Process remaining jobs in channel
			for {
				select {
				case job := <-w.jobChan:
					w.processJob(job)
				default:
					log.Printf("Worker %d stopped", w.id)
					return
				}
			}
			
		case <-w.pool.shutdownChan:
			// Process remaining jobs in channel
			for {
				select {
				case job := <-w.jobChan:
					w.processJob(job)
				default:
					log.Printf("Worker %d stopped due to pool shutdown", w.id)
					return
				}
			}
		}
	}
}

// processJob processes a single job
func (w *Worker) processJob(job Job) {
	atomic.AddInt32(&w.pool.stats.JobsInProgress, 1)
	atomic.AddInt32(&w.pool.stats.ActiveWorkers, 1)
	
	defer func() {
		atomic.AddInt32(&w.pool.stats.JobsInProgress, -1)
		atomic.AddInt32(&w.pool.stats.ActiveWorkers, -1)
		atomic.AddUint64(&w.pool.stats.JobsCompleted, 1)
		
		// Recover from panics in job functions with detailed logging
		if r := recover(); r != nil {
			log.Printf("Worker %d: job %d panicked: %v", w.id, job.ID, r)
			log.Printf("Worker %d: job %d created at: %v", w.id, job.ID, job.Created)
			log.Printf("Worker %d: job %d processing time: %v", w.id, job.ID, time.Since(job.Created))
			
			// Log stack trace for debugging
			log.Printf("Worker %d: job %d stack trace:\n%s", w.id, job.ID, string(debug.Stack()))
		}
	}()
	
	// Execute the job
	job.Function()
}

// GetStats returns current pool statistics
func (p *Pool) GetStats() map[string]interface{} {
	p.stats.mutex.RLock()
	defer p.stats.mutex.RUnlock()
	
	return map[string]interface{}{
		"max_workers":      p.maxWorkers,
		"total_workers":    atomic.LoadInt32(&p.stats.TotalWorkers),
		"active_workers":   atomic.LoadInt32(&p.stats.ActiveWorkers),
		"jobs_submitted":   atomic.LoadUint64(&p.stats.JobsSubmitted),
		"jobs_completed":   atomic.LoadUint64(&p.stats.JobsCompleted),
		"jobs_in_progress": atomic.LoadInt32(&p.stats.JobsInProgress),
		"queue_length":     atomic.LoadInt32(&p.stats.QueueLength),
		"queue_capacity":   cap(p.jobQueue),
		"is_initialized":   p.isInitialized,
		"is_shutdown":      p.isShutdown,
	}
}

// GetQueueLength returns the current length of the job queue
func (p *Pool) GetQueueLength() int {
	return int(atomic.LoadInt32(&p.stats.QueueLength))
}

// GetQueueCapacity returns the capacity of the job queue
func (p *Pool) GetQueueCapacity() int {
	if p.jobQueue == nil {
		return 0
	}
	return cap(p.jobQueue)
}

// GetActiveWorkerCount returns the number of currently active workers
func (p *Pool) GetActiveWorkerCount() int {
	return int(atomic.LoadInt32(&p.stats.ActiveWorkers))
}

// GetJobsInProgress returns the number of jobs currently being processed
func (p *Pool) GetJobsInProgress() int {
	return int(atomic.LoadInt32(&p.stats.JobsInProgress))
}

// GetJobsSubmitted returns the total number of jobs submitted
func (p *Pool) GetJobsSubmitted() uint64 {
	return atomic.LoadUint64(&p.stats.JobsSubmitted)
}

// GetJobsCompleted returns the total number of jobs completed
func (p *Pool) GetJobsCompleted() uint64 {
	return atomic.LoadUint64(&p.stats.JobsCompleted)
}

// IsInitialized returns whether the pool is initialized
func (p *Pool) IsInitialized() bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.isInitialized
}

// IsShutdown returns whether the pool is shut down
func (p *Pool) IsShutdown() bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.isShutdown
}

// GetMaxWorkers returns the maximum number of workers
func (p *Pool) GetMaxWorkers() int {
	return p.maxWorkers
}

// SubmitWithTimeout submits a job with a timeout
func (p *Pool) SubmitWithTimeout(job func(), timeout time.Duration) error {
	if job == nil {
		return fmt.Errorf("job function cannot be nil")
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	
	done := make(chan error, 1)
	go func() {
		done <- p.Submit(job)
	}()
	
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return fmt.Errorf("job submission timed out after %v", timeout)
	}
}

// SubmitWithContext submits a job with context cancellation support
func (p *Pool) SubmitWithContext(ctx context.Context, job func()) error {
	if job == nil {
		return fmt.Errorf("job function cannot be nil")
	}
	
	// Wrap the job to check for context cancellation
	wrappedJob := func() {
		select {
		case <-ctx.Done():
			// Context cancelled, don't execute job
			return
		default:
			job()
		}
	}
	
	return p.Submit(wrappedJob)
}

// WaitForCompletion waits for all submitted jobs to complete
func (p *Pool) WaitForCompletion(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if p.GetJobsInProgress() == 0 && p.GetQueueLength() == 0 {
				return nil
			}
		}
	}
}

// DrainQueue drains all pending jobs from the queue without processing them
func (p *Pool) DrainQueue() int {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if p.jobQueue == nil {
		return 0
	}
	
	drained := 0
	for {
		select {
		case <-p.jobQueue:
			drained++
			atomic.AddInt32(&p.stats.QueueLength, -1)
		default:
			return drained
		}
	}
}

// ResizePool dynamically resizes the worker pool (experimental)
func (p *Pool) ResizePool(newSize int) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if !p.isInitialized || p.isShutdown {
		return fmt.Errorf("cannot resize: pool not initialized or already shutdown")
	}
	
	if newSize <= 0 {
		return fmt.Errorf("new size must be positive, got: %d", newSize)
	}
	
	if newSize == p.maxWorkers {
		return nil // No change needed
	}
	
	// For now, return error as dynamic resizing is complex
	// This would require careful worker lifecycle management
	return fmt.Errorf("dynamic pool resizing not yet implemented")
}

// GetWorkerUtilization returns the worker utilization as a percentage
func (p *Pool) GetWorkerUtilization() float64 {
	totalWorkers := float64(p.GetWorkerCount())
	if totalWorkers == 0 {
		return 0.0
	}
	
	activeWorkers := float64(p.GetActiveWorkerCount())
	return (activeWorkers / totalWorkers) * 100.0
}

// GetQueueUtilization returns the queue utilization as a percentage
func (p *Pool) GetQueueUtilization() float64 {
	capacity := float64(p.GetQueueCapacity())
	if capacity == 0 {
		return 0.0
	}
	
	length := float64(p.GetQueueLength())
	return (length / capacity) * 100.0
}

// GetThroughput returns the job processing throughput (jobs per second)
func (p *Pool) GetThroughput(duration time.Duration) float64 {
	completed := float64(p.GetJobsCompleted())
	seconds := duration.Seconds()
	
	if seconds == 0 {
		return 0.0
	}
	
	return completed / seconds
}

// HealthCheck performs a health check on the worker pool
func (p *Pool) HealthCheck() error {
	if !p.IsInitialized() {
		return fmt.Errorf("worker pool not initialized")
	}
	
	if p.IsShutdown() {
		return fmt.Errorf("worker pool is shut down")
	}
	
	if p.GetWorkerCount() != p.GetMaxWorkers() {
		return fmt.Errorf("worker count mismatch: expected %d, got %d", p.GetMaxWorkers(), p.GetWorkerCount())
	}
	
	// Check if queue is severely backed up
	if p.GetQueueUtilization() > 90.0 {
		return fmt.Errorf("queue utilization too high: %.1f%%", p.GetQueueUtilization())
	}
	
	return nil
}