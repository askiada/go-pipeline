package pipeline

import (
	"time"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

// StepOption changes a step at build time.
// Options are applied when the step is created.
type StepOption[O any] func(s *Step[O])

// RetryPolicy configures per-item retry behaviour for steps.
type RetryPolicy = model.RetryPolicy

// BatchPolicy configures batching/windowing behaviour for batch steps.
type BatchPolicy = model.BatchPolicy

// RateLimitPolicy configures per-item rate limiting for step functions.
type RateLimitPolicy = model.RateLimitPolicy

// StepConcurrency sets how many goroutines run the step.
// Default is 1. Values below 1 are treated as 1.
func StepConcurrency[O any](concurrent int) StepOption[O] {
	return func(s *Step[O]) {
		if concurrent < 1 {
			concurrent = 1
		}

		s.Details.Concurrent = concurrent
	}
}

// StepKeepOpen keeps the output channel open after the step finishes.
// Default behaviour is to close the output channel.
func StepKeepOpen[O any]() StepOption[O] {
	return func(s *Step[O]) {
		s.KeepOpen = true
	}
}

// StepBufferSize sets the output channel buffer size.
// Default is 0 (unbuffered). Larger buffers reduce backpressure but use memory.
func StepBufferSize[O any](bufferSize int) StepOption[O] {
	return func(s *Step[O]) {
		s.Details.BufferSize = bufferSize
	}
}

// StepRetry configures per-item retry behaviour for step functions.
// Default is no retry. MaxAttempts includes the first try; values below 2 disable retries.
func StepRetry[O any](policy RetryPolicy) StepOption[O] {
	return func(step *Step[O]) {
		if policy.MaxAttempts < 2 {
			return
		}

		if policy.Backoff < 0 {
			policy.Backoff = 0
		}

		if policy.MaxBackoff < 0 {
			policy.MaxBackoff = 0
		}

		if policy.Jitter < 0 {
			policy.Jitter = 0
		}

		if policy.Jitter > 1 {
			policy.Jitter = 1
		}

		policyCopy := policy
		step.RetryPolicy = &policyCopy
	}
}

// StepTimeout sets a per-item time limit for step functions.
// Default is no timeout; non-positive values disable the timeout.
func StepTimeout[O any](timeout time.Duration) StepOption[O] {
	return func(step *Step[O]) {
		if timeout <= 0 {
			return
		}

		step.Timeout = timeout
	}
}

// StepRateLimit configures per-item rate limiting for step functions.
// Default is no rate limit. Every must be > 0; Burst is clamped to at least 1.
func StepRateLimit[O any](policy RateLimitPolicy) StepOption[O] {
	return func(step *Step[O]) {
		if policy.Every <= 0 {
			return
		}

		if policy.Burst < 1 {
			policy.Burst = 1
		}

		policyCopy := policy
		step.RateLimitPolicy = &policyCopy
	}
}

// StepMaxInFlight caps the number of in-flight items per step.
// Default is no cap; values below 1 are ignored.
func StepMaxInFlight[O any](maxInFlight int) StepOption[O] {
	return func(step *Step[O]) {
		if maxInFlight < 1 {
			return
		}

		step.MaxInFlight = maxInFlight
	}
}

// StepDropOnFull drops items when the output buffer is full.
// Default is to block, which preserves backpressure.
func StepDropOnFull[O any]() StepOption[O] {
	return func(step *Step[O]) {
		step.DropOnOutputFull = true
	}
}

// StepDropOnBlocked drops items if output sends block longer than the timeout.
// Default is to wait with no drop.
func StepDropOnBlocked[O any](timeout time.Duration) StepOption[O] {
	return func(step *Step[O]) {
		if timeout <= 0 {
			return
		}

		step.DropOnOutputTimeout = timeout
	}
}

// StepDropOnError drops items after retries are exhausted instead of failing the run.
// Default is fail-fast. If error routing is enabled, errors are still routed.
func StepDropOnError[O any]() StepOption[O] {
	return func(step *Step[O]) {
		step.DropOnError = true
	}
}

// StepErrorOutput enables per-item error routing for a step.
// It returns the error step and an option that attaches it to the step.
// The error step name is the step name plus " error". Negative sizes are treated as 0.
func StepErrorOutput[O any](bufferSize int) (*Step[model.StepError], StepOption[O]) {
	if bufferSize < 0 {
		bufferSize = 0
	}

	errorStep := &Step[model.StepError]{
		Details: &StepInfo{
			Type:       model.NormalStepType,
			Concurrent: 1,
			BufferSize: bufferSize,
		},
		Output: make(chan model.StepError, bufferSize),
	}

	return errorStep, func(step *Step[O]) {
		if step == nil {
			return
		}

		if step.Details != nil && errorStep.Details != nil {
			errorStep.Details.Name = step.Details.Name + " error"
		}

		step.ErrorOutputEnabled = true
		step.ErrorOutputBufferSize = bufferSize
		step.ErrorOutput = errorStep.Output
		step.ErrorStep = errorStep
	}
}

// SplitterOption changes a splitter at build time.
type SplitterOption[I any] func(s *Splitter[I])

// SplitterBufferSize sets the buffer size per splitter branch.
// Default is 1 when not set. Larger buffers use more memory.
func SplitterBufferSize[I any](bufferSize int) SplitterOption[I] {
	return func(s *Splitter[I]) {
		s.bufferSize = bufferSize
	}
}
