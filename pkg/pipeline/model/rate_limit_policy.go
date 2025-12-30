package model

import "time"

// RateLimitPolicy configures per-item rate limiting for step functions.
type RateLimitPolicy struct {
	// Every is the minimum duration between allowed items.
	Every time.Duration
	// Burst is the maximum number of items allowed at once.
	Burst int
}
