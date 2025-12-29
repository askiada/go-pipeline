package pipeline

import "github.com/askiada/go-pipeline/pkg/pipeline/model"

// PipelineDefaults configures default step and splitter behavior.
type PipelineDefaults struct {
	StepConcurrency    int
	StepKeepOpen       bool
	StepBufferSize     int
	SplitterBufferSize int
}

func applyStepDefaults[O any](p *Pipeline, step *model.Step[O]) {
	if p == nil || step == nil || step.Details == nil {
		return
	}

	if p.defaults.StepConcurrency > 0 {
		step.Details.Concurrent = p.defaults.StepConcurrency
	}

	if p.defaults.StepBufferSize != 0 {
		step.Details.BufferSize = p.defaults.StepBufferSize
	}

	if p.defaults.StepKeepOpen {
		step.KeepOpen = true
	}
}

func applySplitterDefaults[I any](p *Pipeline, splitter *Splitter[I]) {
	if p == nil || splitter == nil {
		return
	}

	if p.defaults.SplitterBufferSize > 0 {
		splitter.bufferSize = p.defaults.SplitterBufferSize
	}
}
