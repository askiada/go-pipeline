package model

import "time"

// PipelineOption defines the interface for pipeline options.
type PipelineOption interface {
	// New initialises the pipeline option.
	New() error

	pipelineStepOption
	pipelineSpiltterOption
	pipelineMergerOption
	pipelineSinkOption

	// Finish runs after the pipeline is finished.
	Finish() error
}

// pipelineStepOption defines the interface for step options at the pipeline level.
type pipelineStepOption interface {
	// PrepareStep runs before the step is executed.
	PrepareStep(parentStep, step *StepInfo) error
	// OnStepOutput runs everytime something is pushed to the output of the step.
	OnStepOutput(parentStep, step *StepInfo, iterationDuration, computationDuration time.Duration) error
}

// pipelineSpiltterOption defines the interface for splitter options at the pipeline level.
type pipelineSpiltterOption interface {
	// PrepareSplitter runs before the splitter step is executed.
	PrepareSplitter(parentStep, splitterStep *StepInfo) error
	// OnSplitterOutput runs everytime something is pushed to the output of the splitter step.
	OnSplitterOutput(parentStep, splitterStep *StepInfo, iterationDuration, computationDuration time.Duration) error
}

// pipelineMergerOption defines the interface for merger options at the pipeline level.
type pipelineMergerOption interface {
	// PrepareMerger runs before the merger step is executed.
	PrepareMerger(parentStep []*StepInfo, step *StepInfo) error
	// OnMergerOutput runs everytime something is pushed to the output of the merger step.
	OnMergerOutput(parentStep *StepInfo, outputStep *StepInfo, iterationDuration time.Duration) error
}

// pipelineSinkOption defines the interface for sink options at the pipeline level.
type pipelineSinkOption interface {
	// PrepareSink runs before the sink step is executed.
	PrepareSink(parentStep, step *StepInfo) error
	// OnSinkOutput runs everytime something is pushed to the output of the sink step.
	OnSinkOutput(parentStep, step *StepInfo, iterationDuration, computationDuration time.Duration) error
	// AfterSink runs after the sink step is executed.
	AfterSink(step *StepInfo, totalDuration time.Duration) error
}
