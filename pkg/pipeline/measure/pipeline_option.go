package measure

import (
	"time"

	"github.com/askiada/go-pipeline/pkg/pipeline/model"
)

type pipelineMeasure struct {
	Measure
	steps map[string]Metric
}

func (pm *pipelineMeasure) New() error {
	pm.AddMetric(model.StartStep.Details.Name, 1)
	pm.AddMetric(model.EndStep.Details.Name, 1)
	return nil
}

func (pm *pipelineMeasure) PrepareStep(parentStep, step *model.StepInfo) error {
	mt := pm.AddMetric(step.Name, step.Concurrent)
	_ = mt

	return nil
}

func (pm *pipelineMeasure) PrepareSplitter(parentStep, splitterStep *model.StepInfo) error {
	mt := pm.AddMetric(splitterStep.Name, splitterStep.Concurrent)
	_ = mt
	// splitterStep.Metric = mt
	return nil
}

func (pm *pipelineMeasure) PrepareMerger(parentStep []*model.StepInfo, step *model.StepInfo) error {
	mt := pm.AddMetric(step.Name, step.Concurrent)
	_ = mt
	// step.Metric = mt
	return nil
}

func (pm *pipelineMeasure) PrepareSink(parentStep, step *model.StepInfo) error {
	mt := pm.AddMetric(step.Name, step.Concurrent)
	_ = mt
	// step.Metric = mt
	return nil
}

func (pm *pipelineMeasure) Finish() error {
	return nil
}

func (pm *pipelineMeasure) OnStepOutput(parentStep, step *model.StepInfo, iterationDuration, computationDuration time.Duration) error {
	pm.GetMetric(step.Name).AddDuration(computationDuration)
	pm.GetMetric(step.Name).AddTransportDuration(parentStep.Name, iterationDuration)

	return nil
}

func (pm *pipelineMeasure) OnSplitterOutput(parentStep, splitterStep *model.StepInfo, iterationDuration, computationDuration time.Duration) error {
	pm.GetMetric(splitterStep.Name).AddDuration(computationDuration)
	pm.GetMetric(splitterStep.Name).AddTransportDuration(parentStep.Name, iterationDuration)

	return nil
}

func (pm *pipelineMeasure) OnMergerOutput(parentStep *model.StepInfo, outputStep *model.StepInfo, iterationDuration time.Duration) error {
	pm.GetMetric(outputStep.Name).AddTransportDuration(parentStep.Name, iterationDuration)
	return nil
}

func (pm *pipelineMeasure) OnSinkOutput(parentStep, step *model.StepInfo, iterationDuration, computationDuration time.Duration) error {
	pm.GetMetric(step.Name).AddDuration(computationDuration)
	pm.GetMetric(step.Name).AddTransportDuration(parentStep.Name, iterationDuration)

	return nil
}

func (pm *pipelineMeasure) AfterSink(step *model.StepInfo, totalDuration time.Duration) error {
	pm.GetMetric(step.Name).SetTotalDuration(totalDuration)
	return nil
}

func PipelineMeasure(measure Measure) model.PipelineOption {
	return &pipelineMeasure{measure, map[string]Metric{}}
}
