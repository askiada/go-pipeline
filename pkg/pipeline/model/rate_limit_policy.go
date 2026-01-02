package model

import "time"

// RateLimitPolicy configures per-item rate limiting for step functions.
type RateLimitPolicy struct {
	// Every is the minimum duration between allowed items.
	// Values <= 0 disable rate limiting.
	Every time.Duration
	// Burst is the maximum number of items allowed at once.
	// Values below 1 are treated as 1.
	Burst int
}
