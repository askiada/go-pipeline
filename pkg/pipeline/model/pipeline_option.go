package model

import "time"

// PipelineOption is an interface that defines the methods that can be used to customise the behaviour of a pipeline.
// It allows users to define custom behaviour for different steps in the pipeline, such as preparation and output handling.
// Each method corresponds to a specific step type in the pipeline, such as a normal step, splitter, merger, or sink.
// The methods are called at different points in the pipeline execution, allowing users to hook into the pipeline's lifecycle.
type PipelineOption interface {
	// New is called when the pipeline is created.
	// It is used to initialise any resources needed for the pipeline.
	// It is called before any steps are created.
	New() error

	pipelineStepOption
	pipelineSpiltterOption
	pipelineMergerOption
	pipelineSinkOption

	// Finish runs after the pipeline is finished.
	// Finish is called when the pipeline is finished.
	// It is used to perform any cleanup or finalisation needed for the pipeline.
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
