package model

import "time"

type PipelineOption interface {
	New() error
	PrepareStep(parentStep, step *StepInfo) error
	OnStepOutput(parentStep, step *StepInfo, iterationDuration, computationDuration time.Duration) error

	PrepareSplitter(parentStep, splitterStep *StepInfo) error
	OnSplitterOutput(parentStep, splitterStep *StepInfo, iterationDuration, computationDuration time.Duration) error

	PrepareMerger(parentStep []*StepInfo, step *StepInfo) error
	OnMergerOutput(parentStep *StepInfo, outputStep *StepInfo, iterationDuration time.Duration) error

	PrepareSink(parentStep, step *StepInfo) error
	OnSinkOutput(parentStep, step *StepInfo, iterationDuration, computationDuration time.Duration) error

	AfterSink(step *StepInfo, totalDuration time.Duration) error

	Finish() error
}
