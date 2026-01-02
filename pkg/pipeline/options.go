package pipeline

import (
	"time"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

// StepOption is a function that modifies a Step.
type StepOption[O any] func(s *Step[O])

// RetryPolicy configures per-item retry behaviour for steps.
type RetryPolicy = model.RetryPolicy

// BatchPolicy configures batching/windowing behaviour for batch steps.
type BatchPolicy = model.BatchPolicy

// RateLimitPolicy configures per-item rate limiting for step functions.
type RateLimitPolicy = model.RateLimitPolicy

// StepConcurrency sets the concurrency of the step.
func StepConcurrency[O any](concurrent int) StepOption[O] {
	return func(s *Step[O]) {
		if concurrent < 1 {
			concurrent = 1
		}

		s.Details.Concurrent = concurrent
	}
}

// StepKeepOpen does not close input channel.
func StepKeepOpen[O any]() StepOption[O] {
	return func(s *Step[O]) {
		s.KeepOpen = true
	}
}

// StepBufferSize sets the buffer size of the step.
// The output channel of the step will have a buffer of this size.
// If the buffer size is 0, the output channel will be unbuffered.
func StepBufferSize[O any](bufferSize int) StepOption[O] {
	return func(s *Step[O]) {
		s.Details.BufferSize = bufferSize
	}
}

// StepRetry configures per-item retry behaviour for step functions.
// MaxAttempts includes the initial attempt; values below 2 disable retries.
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

// StepTimeout configures a per-item timeout for step functions.
func StepTimeout[O any](timeout time.Duration) StepOption[O] {
	return func(step *Step[O]) {
		if timeout <= 0 {
			return
		}

		step.Timeout = timeout
	}
}

// StepRateLimit configures per-item rate limiting for step functions.
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
func StepMaxInFlight[O any](maxInFlight int) StepOption[O] {
	return func(step *Step[O]) {
		if maxInFlight < 1 {
			return
		}

		step.MaxInFlight = maxInFlight
	}
}

// StepDropOnFull drops items when the output buffer is full.
func StepDropOnFull[O any]() StepOption[O] {
	return func(step *Step[O]) {
		step.DropOnOutputFull = true
	}
}

// StepDropOnBlocked drops items if output sends block longer than the timeout.
func StepDropOnBlocked[O any](timeout time.Duration) StepOption[O] {
	return func(step *Step[O]) {
		if timeout <= 0 {
			return
		}

		step.DropOnOutputTimeout = timeout
	}
}

// StepDropOnError drops items after retries are exhausted instead of propagating the error.
func StepDropOnError[O any]() StepOption[O] {
	return func(step *Step[O]) {
		step.DropOnError = true
	}
}

// StepErrorOutput returns an error step and an option to attach error routing to a step.
// The error step name is derived from the attached step name with an " error" suffix.
func StepErrorOutput[O any](bufferSize int) (*Step[model.StepError], StepOption[O]) {
	if bufferSize < 0 {
		bufferSize = 0
	}

	errorStep := &Step[model.StepError]{
		Details: &model.StepInfo{
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

// SplitterOption is a function that modifies a Splitter.
type SplitterOption[I any] func(s *Splitter[I])

// SplitterBufferSize sets the buffer size of the Splitter. Each splitted step will have a buffer of this size.
func SplitterBufferSize[I any](bufferSize int) SplitterOption[I] {
	return func(s *Splitter[I]) {
		s.bufferSize = bufferSize
	}
}
