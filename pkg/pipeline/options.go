package pipeline

import "github.com/askiada/go-pipeline/pkg/pipeline/model"

type StepOption[O any] func(s *model.Step[O])

func StepConcurrency[O any](concurrent int) StepOption[O] {
	return func(s *model.Step[O]) {
		s.Details.Concurrent = concurrent
	}
}

func StepKeepOpen[O any]() StepOption[O] {
	return func(s *model.Step[O]) {
		s.KeepOpen = true
	}
}

type SplitterOption[I any] func(s *Splitter[I])

func SplitterBufferSize[I any](bufferSize int) SplitterOption[I] {
	return func(s *Splitter[I]) {
		s.bufferSize = bufferSize
	}
}
