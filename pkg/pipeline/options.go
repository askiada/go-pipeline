package pipeline

import "github.com/askiada/go-pipeline/pkg/pipeline/model"

// StepOption is a function that modifies a Step.
type StepOption[O any] func(s *model.Step[O])

// RetryPolicy configures per-item retry behaviour for steps.
type RetryPolicy = model.RetryPolicy

// StepConcurrency sets the concurrency of the step.
func StepConcurrency[O any](concurrent int) StepOption[O] {
	return func(s *model.Step[O]) {
		if concurrent < 1 {
			concurrent = 1
		}

		s.Details.Concurrent = concurrent
	}
}

// StepKeepOpen does not close input channel.
func StepKeepOpen[O any]() StepOption[O] {
	return func(s *model.Step[O]) {
		s.KeepOpen = true
	}
}

// StepBufferSize sets the buffer size of the step.
// The output channel of the step will have a buffer of this size.
// If the buffer size is 0, the output channel will be unbuffered.
func StepBufferSize[O any](bufferSize int) StepOption[O] {
	return func(s *model.Step[O]) {
		s.Details.BufferSize = bufferSize
	}
}

// StepRetry configures per-item retry behaviour for step functions.
// MaxAttempts includes the initial attempt; values below 2 disable retries.
func StepRetry[O any](policy RetryPolicy) StepOption[O] {
	return func(step *model.Step[O]) {
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

// SplitterOption is a function that modifies a Splitter.
type SplitterOption[I any] func(s *Splitter[I])

// SplitterBufferSize sets the buffer size of the Splitter. Each splitted step will have a buffer of this size.
func SplitterBufferSize[I any](bufferSize int) SplitterOption[I] {
	return func(s *Splitter[I]) {
		s.bufferSize = bufferSize
	}
}
