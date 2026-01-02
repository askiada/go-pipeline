package model

import "time"

// BatchPolicy configures batching and windowing for batch steps.
type BatchPolicy struct {
	// MaxSize is the maximum number of items per batch. It must be >= 1.
	MaxSize int
	// MaxWait is the maximum time to wait before flushing a partial batch.
	// Values <= 0 disable time-based flushing.
	MaxWait time.Duration
}
