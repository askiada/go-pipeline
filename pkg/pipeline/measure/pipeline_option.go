package measure

import (
	"time"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline"
	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

type pipelineMeasure struct {
	Measure
}

// New creates a new pipeline measure.
func (pm *pipelineMeasure) New() error {
	pm.AddMetric(model.StartStep.Details.Name, 1)
	pm.AddMetric(model.EndStep.Details.Name, 1)

	return nil
}

// PrepareStep is called before the step is executed.
func (pm *pipelineMeasure) PrepareStep(_, step *pipeline.StepInfo) error {
	pm.AddMetric(step.Name, step.Concurrent)

	return nil
}

// PrepareSplitter is called before the splitter step is executed.
func (pm *pipelineMeasure) PrepareSplitter(_, splitterStep *pipeline.StepInfo) error {
	pm.AddMetric(splitterStep.Name, splitterStep.Concurrent)

	return nil
}

// PrepareMerger is called before the merger step is executed.
func (pm *pipelineMeasure) PrepareMerger(_ []*pipeline.StepInfo, step *pipeline.StepInfo) error {
	pm.AddMetric(step.Name, step.Concurrent)

	return nil
}

// PrepareSink is called before the sink step is executed.
func (pm *pipelineMeasure) PrepareSink(_, step *pipeline.StepInfo) error {
	pm.AddMetric(step.Name, step.Concurrent)

	return nil
}

// Finish is called after the pipeline is finished.
func (pm *pipelineMeasure) Finish() error {
	return nil
}

// OnStepOutput is called after each step output is processed.
func (pm *pipelineMeasure) OnStepOutput(_, _ *pipeline.StepInfo) error {
	return nil
}

// OnStepRetry is called after a retry attempt fails.
//
//nolint:unparam // Required by the pipeline option interface.
func (pm *pipelineMeasure) OnStepRetry(_, step *pipeline.StepInfo, _ int, computationDuration time.Duration) error {
	if metric, ok := pm.GetMetric(step.Name).(RetryMetric); ok {
		metric.AddRetryDuration(computationDuration)
	}

	return nil
}

// OnStepDrop is called after an item is dropped.
//
//nolint:unparam // Required by the pipeline option interface.
func (pm *pipelineMeasure) OnStepDrop(step *pipeline.StepInfo, kind model.StepDropKind) error {
	if metric, ok := pm.GetMetric(step.Name).(DropMetric); ok {
		metric.AddDrop(kind)
	}

	return nil
}

// OnStepErrorRoute is called after an item is routed to the error channel.
//
//nolint:unparam // Required by the pipeline option interface.
func (pm *pipelineMeasure) OnStepErrorRoute(step *pipeline.StepInfo) error {
	if metric, ok := pm.GetMetric(step.Name).(DropMetric); ok {
		metric.AddRoutedError()
	}

	return nil
}

// OnSplitterOutput is called after each splitter step output is processed.
func (pm *pipelineMeasure) OnSplitterOutput(_, _ *pipeline.StepInfo) error {
	return nil
}

// OnMergerOutput is called after each merger step output is processed.
func (pm *pipelineMeasure) OnMergerOutput(_, _ *pipeline.StepInfo) error {
	return nil
}

// OnSinkOutput is called after each sink step output is processed.
func (pm *pipelineMeasure) OnSinkOutput(_, _ *pipeline.StepInfo) error {
	return nil
}

// AfterSink is called after the sink step is executed.
func (pm *pipelineMeasure) AfterSink(_ *pipeline.StepInfo) error {
	return nil
}

// OnStepOutputMetrics is called after each step output is processed.
//
//nolint:unparam // Required by the pipeline option interface.
func (pm *pipelineMeasure) OnStepOutputMetrics(
	parentStep, step *pipeline.StepInfo,
	iterationDuration, computationDuration time.Duration,
) error {
	pm.GetMetric(step.Name).AddDuration(computationDuration)
	pm.GetMetric(step.Name).AddTransportDuration(parentStep.Name, iterationDuration)

	return nil
}

// OnSplitterOutputMetrics is called after each splitter step output is processed.
//
//nolint:unparam // Required by the pipeline option interface.
func (pm *pipelineMeasure) OnSplitterOutputMetrics(
	parentStep, splitterStep *pipeline.StepInfo,
	iterationDuration, computationDuration time.Duration,
) error {
	pm.GetMetric(splitterStep.Name).AddDuration(computationDuration)
	pm.GetMetric(splitterStep.Name).AddTransportDuration(parentStep.Name, iterationDuration)

	return nil
}

// OnMergerOutputMetrics is called after each merger step output is processed.
//
//nolint:unparam // Required by the pipeline option interface.
func (pm *pipelineMeasure) OnMergerOutputMetrics(
	parentStep, outputStep *pipeline.StepInfo,
	iterationDuration time.Duration,
) error {
	pm.GetMetric(outputStep.Name).AddTransportDuration(parentStep.Name, iterationDuration)

	return nil
}

// OnSinkOutputMetrics is called after each sink step output is processed.
//
//nolint:unparam // Required by the pipeline option interface.
func (pm *pipelineMeasure) OnSinkOutputMetrics(
	parentStep, step *pipeline.StepInfo,
	iterationDuration, computationDuration time.Duration,
) error {
	pm.GetMetric(step.Name).AddDuration(computationDuration)
	pm.GetMetric(step.Name).AddTransportDuration(parentStep.Name, iterationDuration)

	return nil
}

// AfterSinkMetrics is called after the sink step is executed.
//
//nolint:unparam // Required by the pipeline option interface.
func (pm *pipelineMeasure) AfterSinkMetrics(step *pipeline.StepInfo, totalDuration time.Duration) error {
	pm.GetMetric(step.Name).SetTotalDuration(totalDuration)

	return nil
}

// PipelineMeasure returns a pipeline option that records metrics.
func PipelineMeasure(measure Measure) model.PipelineOption { //nolint:ireturn // it must implement the interface
	return &pipelineMeasure{Measure: measure}
}
