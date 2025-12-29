package pipeline

import (
	"time"

	"github.com/askiada/go-pipeline/pkg/pipeline/model"
)

// PipelineDefaults configures default step and splitter behaviour.
type PipelineDefaults struct {
	StepConcurrency    int
	StepKeepOpen       bool
	StepBufferSize     int
	SplitterBufferSize int
}

func (PipelineDefaults) New() error {
	return nil
}

func (PipelineDefaults) Finish() error {
	return nil
}

func (PipelineDefaults) PrepareStep(_, _ *model.StepInfo) error {
	return nil
}

func (PipelineDefaults) OnStepOutput(_, _ *model.StepInfo, _, _ time.Duration) error {
	return nil
}

func (PipelineDefaults) PrepareSplitter(_, _ *model.StepInfo) error {
	return nil
}

func (PipelineDefaults) OnSplitterOutput(_, _ *model.StepInfo, _, _ time.Duration) error {
	return nil
}

func (PipelineDefaults) PrepareMerger(_ []*model.StepInfo, _ *model.StepInfo) error {
	return nil
}

func (PipelineDefaults) OnMergerOutput(_, _ *model.StepInfo, _ time.Duration) error {
	return nil
}

func (PipelineDefaults) PrepareSink(_, _ *model.StepInfo) error {
	return nil
}

func (PipelineDefaults) OnSinkOutput(_, _ *model.StepInfo, _, _ time.Duration) error {
	return nil
}

func (PipelineDefaults) AfterSink(_ *model.StepInfo, _ time.Duration) error {
	return nil
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
