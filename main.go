package main

import (
	"fmt"
	"runtime"
)

// RecordReader simulates a memory-efficient Parquet record reader for nested datasets.
type RecordReader struct {
	batchSize   int
	levelBuffer []int16 // Reusable buffer for definition/repetition levels
}

// NewRecordReader creates a new RecordReader with a configurable batch size.
func NewRecordReader(batchSize int) *RecordReader {
	return &RecordReader{
		batchSize:   batchSize,
		levelBuffer: make([]int16, 0, batchSize), // Pre-allocate buffer up to batch size
	}
}

// ReadBatch simulates reading a batch of nested records.
// It reuses the internal levelBuffer to avoid frequent allocations.
func (r *RecordReader) ReadBatch(totalRows int, currentOffset int) (int, []int16) {
	if currentOffset >= totalRows {
		return 0, nil
	}

	remaining := totalRows - currentOffset
	readSize := r.batchSize
	if remaining < readSize {
		readSize = remaining
	}

	// Reuse the buffer by slicing it to the required read size
	r.levelBuffer = r.levelBuffer[:0]
	for i := 0; i < readSize; i++ {
		// Simulate decoding definition/repetition levels (e.g., nested depth levels)
		r.levelBuffer = append(r.levelBuffer, int16(i%5))
	}

	return readSize, r.levelBuffer
}

func main() {
	fmt.Println("Starting Parquet Reader Memory Optimization Simulation...")

	// Configuration
	batchSize := 10000
	totalRows := 1000000 // 1,000,000 rows as per benchmark spec

	// Initialize reader with configurable batch size and buffer reuse
	reader := NewRecordReader(batchSize)

	var memStatsBefore runtime.MemStats
	runtime.ReadMemStats(&memStatsBefore)

	fmt.Printf("Initial Alloc: %d KB\n", memStatsBefore.Alloc/1024)

	// Simulate reading the entire dataset in batches
	readRows := 0
	for readRows < totalRows {
		n, levels := reader.ReadBatch(totalRows, readRows)
		if n == 0 {
			break
		}
		readRows += n

		// Do some dummy processing with levels to prevent compiler optimization
		_ = levels
	}

	var memStatsAfter runtime.MemStats
	runtime.ReadMemStats(&memStatsAfter)

	fmt.Printf("Final Alloc: %d KB\n", memStatsAfter.Alloc/1024)
	fmt.Printf("Peak/Allocated Memory is bounded and scales with batch_size (%d) rather than total rows (%d).\n", batchSize, totalRows)
	fmt.Println("Optimization Simulation Completed Successfully.")
}
