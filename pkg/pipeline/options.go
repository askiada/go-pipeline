package pipeline

import "github.com/askiada/go-pipeline/pkg/pipeline/model"

// StepOption is a function that modifies a Step.
type StepOption[O any] func(s *model.Step[O])

// StepConcurrency sets the concurrency of the step.
func StepConcurrency[O any](concurrent int) StepOption[O] {
	return func(s *model.Step[O]) {
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

// SplitterOption is a function that modifies a Splitter.
type SplitterOption[I any] func(s *Splitter[I])

// SplitterBufferSize sets the buffer size of the Splitter. Each splitted step will have a buffer of this size.
func SplitterBufferSize[I any](bufferSize int) SplitterOption[I] {
	return func(s *Splitter[I]) {
		s.bufferSize = bufferSize
	}
}
