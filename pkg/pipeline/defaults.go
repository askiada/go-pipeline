package pipeline

import (
	"time"

	"github.com/askiada/go-pipeline/pkg/pipeline/model"
)

// PipelineDefaults configures default step and splitter behaviour.
//
//nolint:revive // Name is part of the public API.
type PipelineDefaults struct {
	StepConcurrency    int
	StepKeepOpen       bool
	StepBufferSize     int
	SplitterBufferSize int
}

// New implements model.PipelineOption.
func (PipelineDefaults) New() error {
	return nil
}

// Finish implements model.PipelineOption.
func (PipelineDefaults) Finish() error {
	return nil
}

// PrepareStep implements model.PipelineOption.
func (PipelineDefaults) PrepareStep(_, _ *model.StepInfo) error {
	return nil
}

// OnStepOutput implements model.PipelineOption.
func (PipelineDefaults) OnStepOutput(_, _ *model.StepInfo, _, _ time.Duration) error {
	return nil
}

// PrepareSplitter implements model.PipelineOption.
func (PipelineDefaults) PrepareSplitter(_, _ *model.StepInfo) error {
	return nil
}

// OnSplitterOutput implements model.PipelineOption.
func (PipelineDefaults) OnSplitterOutput(_, _ *model.StepInfo, _, _ time.Duration) error {
	return nil
}

// PrepareMerger implements model.PipelineOption.
func (PipelineDefaults) PrepareMerger(_ []*model.StepInfo, _ *model.StepInfo) error {
	return nil
}

// OnMergerOutput implements model.PipelineOption.
func (PipelineDefaults) OnMergerOutput(_, _ *model.StepInfo, _ time.Duration) error {
	return nil
}

// PrepareSink implements model.PipelineOption.
func (PipelineDefaults) PrepareSink(_, _ *model.StepInfo) error {
	return nil
}

// OnSinkOutput implements model.PipelineOption.
func (PipelineDefaults) OnSinkOutput(_, _ *model.StepInfo, _, _ time.Duration) error {
	return nil
}

// AfterSink implements model.PipelineOption.
func (PipelineDefaults) AfterSink(_ *model.StepInfo, _ time.Duration) error {
	return nil
}

func applyStepDefaults[O any](pipe *Pipeline, step *model.Step[O]) {
	if pipe == nil || step == nil || step.Details == nil {
		return
	}

	if pipe.defaults.StepConcurrency > 0 {
		step.Details.Concurrent = pipe.defaults.StepConcurrency
	}

	if pipe.defaults.StepBufferSize != 0 {
		step.Details.BufferSize = pipe.defaults.StepBufferSize
	}

	if pipe.defaults.StepKeepOpen {
		step.KeepOpen = true
	}
}

func applySplitterDefaults[I any](pipe *Pipeline, splitter *Splitter[I]) {
	if pipe == nil || splitter == nil {
		return
	}

	if pipe.defaults.SplitterBufferSize > 0 {
		splitter.bufferSize = pipe.defaults.SplitterBufferSize
	}
}
