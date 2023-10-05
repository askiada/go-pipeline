package pipeline

import (
	"time"

	"github.com/askiada/go-pipeline/pkg/pipeline/drawer"
	"github.com/askiada/go-pipeline/pkg/pipeline/measure"
	"github.com/pkg/errors"
)

type PipelineOption interface {
	New() error
	BeforeStep(parentStepName, stepName *StepInfo) error
	BeforeSplitter(parentStep, splitterStep *StepInfo) error
	BeforeMerger(parentStep []*StepInfo, step *StepInfo) error
	BeforeSink(parentStep, step *StepInfo) error
	Finish() error
}

type pipelineDrawer struct {
	drawer.Drawer
	m         measure.Measure
	startTime time.Time
}

func (pd *pipelineDrawer) New() error {
	err := pd.AddStep(startStep.details.Name)
	if err != nil {
		return errors.Wrap(err, "unable to add start step to drawer")
	}
	err = pd.AddStep(endStep.details.Name)
	if err != nil {
		return errors.Wrap(err, "unable to add end step to drawer")
	}

	return nil
}

func (pd *pipelineDrawer) BeforeStep(parentStep, step *StepInfo) error {
	err := pd.AddStep(step.Name)
	if err != nil {
		return err
	}
	err = pd.AddLink(parentStep.Name, step.Name)
	if err != nil {
		return err
	}

	return nil
}

func (pd *pipelineDrawer) BeforeSplitter(parentStep, splitterStep *StepInfo) error {
	err := pd.AddStep(splitterStep.Name)
	if err != nil {
		return err
	}
	err = pd.AddLink(parentStep.Name, splitterStep.Name)
	if err != nil {
		return err
	}

	return nil
}

func (pd *pipelineDrawer) BeforeMerger(parentStep []*StepInfo, step *StepInfo) error {
	err := pd.AddStep(step.Name)
	if err != nil {
		return err
	}

	for _, parentStep := range parentStep {
		err := pd.AddLink(parentStep.Name, step.Name)
		if err != nil {
			return err
		}
	}
	return nil
}

func (pd *pipelineDrawer) BeforeSink(parentStep, step *StepInfo) error {
	err := pd.AddStep(step.Name)
	if err != nil {
		return err
	}
	err = pd.AddLink(parentStep.Name, step.Name)
	if err != nil {
		return err
	}
	err = pd.AddLink(step.Name, endStep.details.Name)
	if err != nil {
		return err
	}

	return nil
}

func (pd *pipelineDrawer) Finish() error {
	if pd.m != nil {
		err := pd.SetTotalTime(endStep.details.Name, pd.startTime)
		if err != nil {
			return errors.Wrap(err, "unable to set total time")
		}
		err = pd.AddMeasure(pd.m)
		if err != nil {
			return errors.Wrap(err, "unable to add measure")
		}
	}

	err := pd.Draw()
	if err != nil {
		return errors.Wrap(err, "unable to draw pipeline")
	}

	return nil
}

func PipelineDrawer(drawer drawer.Drawer, measure measure.Measure) PipelineOption {
	return &pipelineDrawer{drawer, measure, time.Now()}
}

type pipelineMeasure struct {
	measure.Measure
}

func (pm *pipelineMeasure) New() error {
	pm.AddMetric(startStep.details.Name, 1)
	pm.AddMetric(endStep.details.Name, 1)
	return nil
}

func (pm *pipelineMeasure) BeforeStep(parentStep, step *StepInfo) error {
	mt := pm.AddMetric(step.Name, step.Concurrent)
	step.Metric = mt
	return nil
}

func (pm *pipelineMeasure) BeforeSplitter(parentStep, splitterStep *StepInfo) error {
	mt := pm.AddMetric(splitterStep.Name, splitterStep.Concurrent)
	splitterStep.Metric = mt
	return nil
}

func (pm *pipelineMeasure) BeforeMerger(parentStep []*StepInfo, step *StepInfo) error {
	mt := pm.AddMetric(step.Name, step.Concurrent)
	step.Metric = mt
	return nil
}

func (pm *pipelineMeasure) BeforeSink(parentStep, step *StepInfo) error {
	mt := pm.AddMetric(step.Name, step.Concurrent)
	step.Metric = mt
	return nil
}

func (pm *pipelineMeasure) Finish() error {
	return nil
}

func PipelineMeasure(measure measure.Measure) PipelineOption {
	return &pipelineMeasure{measure}
}

type StepOption[O any] func(s *Step[O])

func StepConcurrency[O any](concurrent int) StepOption[O] {
	return func(s *Step[O]) {
		s.details.Concurrent = concurrent
	}
}

func StepKeepOpen[O any]() StepOption[O] {
	return func(s *Step[O]) {
		s.keepOpen = true
	}
}

type SplitterOption[I any] func(s *Splitter[I])

func SplitterBufferSize[I any](bufferSize int) SplitterOption[I] {
	return func(s *Splitter[I]) {
		s.bufferSize = bufferSize
	}
}
