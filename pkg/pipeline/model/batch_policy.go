package model

import "time"

// BatchPolicy configures batching/windowing behaviour for batch steps.
type BatchPolicy struct {
	// MaxSize is the maximum number of items per batch.
	MaxSize int
	// MaxWait is the maximum time to wait before flushing a partial batch.
	MaxWait time.Duration
}
