package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/despreston/go-craq/transport"
	"github.com/despreston/go-craq/transport/netrpc"
)

// Operation types
const (
	OperationWrite = "write"
	OperationRead  = "read"
)

// WorkloadConfig defines how the client should operate
type WorkloadConfig struct {
	// Coordinator connection details
	CoordinatorAddr string
	
	// Operation types and percentages
	WritePercentage int
	ReadPercentage  int
	
	// Concurrency parameters
	NumOperations   int
	NumConcurrent   int
	
	// Timing parameters
	ThinkTime       time.Duration
	Timeout         time.Duration
	
	// Benchmark duration (alternative to NumOperations)
	Duration        time.Duration
	
	// Data parameters
	KeyPrefix       string
	KeyRange        int
	ValueSize       int
	
	// Whether to report operation statistics
	Verbose         bool
}

// Result captures statistics for a single operation
type Result struct {
	Operation   string
	Key         string
	StartTime   time.Time
	Duration    time.Duration
	Success     bool
	ErrorMsg    string
}

// Stats tracks aggregate statistics for all operations
type Stats struct {
	sync.Mutex
	TotalOps         int
	SuccessfulOps    int
	FailedOps        int
	TotalWriteOps    int
	SuccessfulWrites int
	FailedWrites     int
	TotalReadOps     int
	SuccessfulReads  int
	FailedReads      int
	WriteLatencies   []time.Duration
	ReadLatencies    []time.Duration
	Errors           []string
}

func main() {
	// Basic connection parameters
	var cdrHost, cdrPort string
	var writePercentage, readPercentage int
	var numOperations, numConcurrent int
	var thinkTimeMs, timeoutMs, durationSec int
	var keyPrefix string
	var keyRange, valueSize int
	
	flag.StringVar(&cdrHost, "ch", "localhost", "Coordinator hostname")
	flag.StringVar(&cdrPort, "cp", "1234", "Coordinator port")
	flag.IntVar(&writePercentage, "write", 50, "Percentage of write operations (0-100)")
	flag.IntVar(&readPercentage, "read", 50, "Percentage of read operations (0-100)")
	flag.IntVar(&numOperations, "n", 500, "Total number of operations to perform")
	flag.IntVar(&numConcurrent, "c", 1, "Number of concurrent clients")
	flag.IntVar(&thinkTimeMs, "think", 0, "Think time between operations in milliseconds")
	flag.IntVar(&timeoutMs, "timeout", 5000, "Operation timeout in milliseconds")
	flag.IntVar(&durationSec, "duration", 0, "Run for specified seconds instead of fixed operations count (0 = use -n)")
	flag.StringVar(&keyPrefix, "key-prefix", "key", "Prefix for generated keys")
	flag.IntVar(&keyRange, "key-range", 1000, "Range of keys to use")
	flag.IntVar(&valueSize, "value-size", 100, "Size of values in bytes")
	
	// Other options
	var verbose bool
	flag.BoolVar(&verbose, "v", false, "Verbose output")
	
	// Parse command line flags
	flag.Parse()

	// Validate and prepare configuration
	if writePercentage+readPercentage != 100 {
		log.Fatal("Error: Write percentage + read percentage must equal 100")
	}
	
	cdrAddr := fmt.Sprintf("%s:%s", cdrHost, cdrPort)
	
	// Create workload configuration
	config := WorkloadConfig{
		CoordinatorAddr: cdrAddr,
		WritePercentage: writePercentage,
		ReadPercentage:  readPercentage,
		NumOperations:   numOperations,
		NumConcurrent:   numConcurrent,
		ThinkTime:       time.Duration(thinkTimeMs) * time.Millisecond,
		Timeout:         time.Duration(timeoutMs) * time.Millisecond,
		Duration:        time.Duration(durationSec) * time.Second,
		KeyPrefix:       keyPrefix,
		KeyRange:        keyRange,
		ValueSize:       valueSize,
		Verbose:         verbose,
	}
	
	// Run the benchmark
	runBenchmark(config)
}

// runBenchmark executes a configurable benchmark workload against the CRAQ cluster
func runBenchmark(config WorkloadConfig) {
	rand.Seed(time.Now().UnixNano())
	stats := &Stats{
		WriteLatencies: make([]time.Duration, 0, config.NumOperations),
		ReadLatencies:  make([]time.Duration, 0, config.NumOperations),
		Errors:         make([]string, 0),
	}

	log.Printf("Connecting to coordinator at %s", config.CoordinatorAddr)

	// Connect to coordinator once to get its client
	coordinator := netrpc.NewCoordinatorClient()
	if err := coordinator.Connect(config.CoordinatorAddr); err != nil {
		log.Fatalf("Failed to connect to coordinator: %v", err)
	}
	defer coordinator.Close()

	// Connect to tail node for reads (required for vanilla chain replication)
	var tailAddr string
	if config.ReadPercentage > 0 {
		var err error
		tailAddr, err = coordinator.GetTailAddress()
		if err != nil {
			log.Fatalf("Failed to get tail node address: %v", err)
		}
		log.Printf("Using tail node at %s for read operations", tailAddr)
	}

	// For tracking benchmark progress and reporting
	startTime := time.Now()
	var endTime time.Time
	if config.Duration > 0 {
		endTime = startTime.Add(config.Duration)
		log.Printf("Running benchmark for %v", config.Duration)
	} else {
		log.Printf("Running benchmark for %d operations", config.NumOperations)
	}

	// Create a channel to receive operation results
	results := make(chan Result, config.NumOperations)
	
	// Create a channel to control the worker goroutines
	stopWorkers := make(chan struct{})
	
	// waitgroup for workers
	var wg sync.WaitGroup

	// Launch worker goroutines
	log.Printf("Starting %d concurrent clients", config.NumConcurrent)
	for i := 0; i < config.NumConcurrent; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			// Each worker needs its own coordinator client
			workerCoordinator := netrpc.NewCoordinatorClient()
			if err := workerCoordinator.Connect(config.CoordinatorAddr); err != nil {
				log.Printf("Worker %d failed to connect to coordinator: %v", id, err)
				return
			}
			defer workerCoordinator.Close()
			
			// Create a tail node client if we need to do reads
			var tailClient transport.NodeClient
			if config.ReadPercentage > 0 {
				nodeClient := netrpc.NewNodeClient()
				if err := nodeClient.Connect(tailAddr); err != nil {
					log.Printf("Worker %d failed to connect to tail node: %v", id, err)
					return
				}
				defer nodeClient.Close()
				tailClient = nodeClient
			}
			
			// Prepare value for write operations
			writeValue := make([]byte, config.ValueSize)
			for i := 0; i < config.ValueSize; i++ {
				writeValue[i] = byte(65 + (i % 26)) // ASCII values for A-Z
			}
			
			for {
				select {
				case <-stopWorkers:
					return
				default:
					// Generate operation type based on configured percentages
					opType := OperationWrite
					if config.ReadPercentage > 0 && config.WritePercentage > 0 {
						// Mixed workload
						if rand.Intn(100) < config.ReadPercentage {
							opType = OperationRead
						}
					} else if config.ReadPercentage == 100 {
						// Read-only workload
						opType = OperationRead
					} // else it remains write-only
					
					// Generate a random key within the configured range
					key := fmt.Sprintf("%s%d", config.KeyPrefix, rand.Intn(config.KeyRange))
					
					// Execute the operation
					result := executeOperation(opType, key, writeValue, workerCoordinator, tailClient, config.Timeout)
					results <- result
					
					// Simulate think time between operations if configured
					if config.ThinkTime > 0 {
						time.Sleep(config.ThinkTime)
					}
				}
			}
		}(i)
	}
	
	// Process results
	go func() {
		opCount := 0
		
		// Process results until we reach the target count or duration
		for result := range results {
			opCount++
			processResult(result, stats)
			
			if config.Verbose {
				log.Printf("[%s] %s: %v (success=%v)", 
					result.Operation, result.Key, result.Duration, result.Success)
			}
			
			// Check if we should stop based on operation count
			if config.Duration == 0 && opCount >= config.NumOperations {
				break
			}
			
			// Check if we should stop based on duration
			if config.Duration > 0 && time.Now().After(endTime) {
				break
			}
			
			// Periodic status report every 1000 operations
			if opCount%1000 == 0 {
				elapsed := time.Since(startTime)
				opsPerSec := float64(opCount) / elapsed.Seconds()
				log.Printf("Progress: %d operations, %.2f ops/sec", opCount, opsPerSec)
			}
		}
		
		// Signal workers to stop
		close(stopWorkers)
	}()
	
	// Wait for workers to finish
	wg.Wait()
	close(results)
	
	// Print final statistics
	printStats(stats, time.Since(startTime))
}

// executeOperation performs a single read or write operation
func executeOperation(opType, key string, value []byte, coordinator transport.CoordinatorClient, 
	tailClient transport.NodeClient, timeout time.Duration) Result {
	
	result := Result{
		Operation: opType,
		Key:       key,
		StartTime: time.Now(),
	}
	
	// Set up timeout context
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	
	// Execute operation with timeout
	errChan := make(chan error, 1)
	
	if opType == OperationWrite {
		go func() {
			err := coordinator.Write(key, value)
			errChan <- err
		}()
	} else { // OperationRead
		go func() {
			_, _, err := tailClient.Read(key)
			errChan <- err
		}()
	}
	
	// Wait for operation to complete or timeout
	select {
	case err := <-errChan:
		result.Duration = time.Since(result.StartTime)
		if err != nil {
			result.Success = false
			result.ErrorMsg = err.Error()
		} else {
			result.Success = true
		}
	case <-ctx.Done():
		result.Duration = time.Since(result.StartTime)
		result.Success = false
		result.ErrorMsg = "operation timed out"
	}
	
	return result
}

// processResult updates statistics based on a single operation result
func processResult(result Result, stats *Stats) {
	stats.Lock()
	defer stats.Unlock()
	
	stats.TotalOps++
	if result.Success {
		stats.SuccessfulOps++
	} else {
		stats.FailedOps++
		stats.Errors = append(stats.Errors, result.ErrorMsg)
	}
	
	if result.Operation == OperationWrite {
		stats.TotalWriteOps++
		if result.Success {
			stats.SuccessfulWrites++
		} else {
			stats.FailedWrites++
		}
		stats.WriteLatencies = append(stats.WriteLatencies, result.Duration)
	} else {
		stats.TotalReadOps++
		if result.Success {
			stats.SuccessfulReads++
		} else {
			stats.FailedReads++
		}
		stats.ReadLatencies = append(stats.ReadLatencies, result.Duration)
	}
}

// printStats outputs the final benchmark statistics
func printStats(stats *Stats, duration time.Duration) {
	log.Println("========== BENCHMARK RESULTS ==========")
	log.Printf("Total duration: %v", duration)
	log.Printf("Total operations: %d", stats.TotalOps)
	
	opsPerSec := float64(stats.TotalOps) / duration.Seconds()
	log.Printf("Operations per second: %.2f", opsPerSec)
	
	if stats.TotalOps > 0 {
		log.Printf("Success rate: %.2f%% (%d/%d)", 
			float64(stats.SuccessfulOps)/float64(stats.TotalOps)*100.0,
			stats.SuccessfulOps, stats.TotalOps)
	}
	
	// Compute write statistics
	if stats.TotalWriteOps > 0 {
		avgWriteLatency := calculateAvgLatency(stats.WriteLatencies)
		p95WriteLatency := calculatePercentileLatency(stats.WriteLatencies, 95)
		p99WriteLatency := calculatePercentileLatency(stats.WriteLatencies, 99)
		
		log.Printf("WRITES: %d total, %d successful (%.2f%%), %d failed", 
			stats.TotalWriteOps, stats.SuccessfulWrites, 
			float64(stats.SuccessfulWrites)/float64(stats.TotalWriteOps)*100.0,
			stats.FailedWrites)
		log.Printf("Write latency: avg=%v, p95=%v, p99=%v", 
			avgWriteLatency, p95WriteLatency, p99WriteLatency)
	}
	
	// Compute read statistics
	if stats.TotalReadOps > 0 {
		avgReadLatency := calculateAvgLatency(stats.ReadLatencies)
		p95ReadLatency := calculatePercentileLatency(stats.ReadLatencies, 95)
		p99ReadLatency := calculatePercentileLatency(stats.ReadLatencies, 99)
		
		log.Printf("READS: %d total, %d successful (%.2f%%), %d failed", 
			stats.TotalReadOps, stats.SuccessfulReads,
			float64(stats.SuccessfulReads)/float64(stats.TotalReadOps)*100.0,
			stats.FailedReads)
		log.Printf("Read latency: avg=%v, p95=%v, p99=%v", 
			avgReadLatency, p95ReadLatency, p99ReadLatency)
	}
	
	// Print some error samples if there were failures
	if len(stats.Errors) > 0 {
		log.Printf("Sample errors (%d total):", len(stats.Errors))
		// Print up to 5 unique errors with counts
		uniqueErrors := make(map[string]int)
		for _, err := range stats.Errors {
			uniqueErrors[err]++
		}
		
		// Sort errors by frequency (most common first)
		type errorCount struct {
			message string
			count   int
		}
		
		errorCounts := make([]errorCount, 0, len(uniqueErrors))
		for msg, count := range uniqueErrors {
			errorCounts = append(errorCounts, errorCount{msg, count})
		}
		
		sort.Slice(errorCounts, func(i, j int) bool {
			return errorCounts[i].count > errorCounts[j].count
		})
		
		// Print top 5 errors (or fewer if less than 5 exist)
		limit := 5
		if len(errorCounts) < limit {
			limit = len(errorCounts)
		}
		
		for i := 0; i < limit; i++ {
			err := errorCounts[i]
			log.Printf("  - %s (occurred %d times)", err.message, err.count)
		}
	}
	
	log.Println("======================================")
}

// calculateAvgLatency calculates the average of all latencies
func calculateAvgLatency(latencies []time.Duration) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	
	var sum time.Duration
	for _, d := range latencies {
		sum += d
	}
	return time.Duration(int64(sum) / int64(len(latencies)))
}

// calculatePercentileLatency calculates the nth percentile of latencies
func calculatePercentileLatency(latencies []time.Duration, percentile int) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	
	// Make a copy and sort it
	sortedLatencies := make([]time.Duration, len(latencies))
	copy(sortedLatencies, latencies)
	sort.Slice(sortedLatencies, func(i, j int) bool {
		return sortedLatencies[i] < sortedLatencies[j]
	})
	
	// Calculate the index for the percentile
	idx := int(float64(len(sortedLatencies)) * float64(percentile) / 100.0)
	if idx >= len(sortedLatencies) {
		idx = len(sortedLatencies) - 1
	}
	
	return sortedLatencies[idx]
}
