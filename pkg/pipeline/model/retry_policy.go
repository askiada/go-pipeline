package model

import "time"

// RetryPolicy configures per-item retry behaviour for steps.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts, including the first try.
	// Values below 2 disable retries.
	MaxAttempts int
	// Backoff is the base delay between retry attempts. Zero means no delay.
	Backoff time.Duration
	// MaxBackoff caps the backoff delay when set.
	MaxBackoff time.Duration
	// Jitter is a ratio (0..1) applied to backoff delays.
	Jitter float64
	// RetryOn decides whether an error should be retried.
	// When nil, context cancel and deadline errors are not retried.
	RetryOn func(error) bool
}
