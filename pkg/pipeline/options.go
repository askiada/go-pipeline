package pipeline

import (
	"github.com/askiada/go-pipeline/pkg/pipeline/drawer"
	"github.com/askiada/go-pipeline/pkg/pipeline/measure"
	"github.com/pkg/errors"
)

type PipelineOption interface {
	Init(p *Pipeline) error
	Finish(p *Pipeline) error
}

type pipelineDrawer struct {
	drawer.Drawer
}

func (pd *pipelineDrawer) Init(p *Pipeline) error {
	p.drawer = pd
	err := pd.AddStep(startStepName)
	if err != nil {
		return errors.Wrap(err, "unable to add start step to drawer")
	}
	err = pd.AddStep(endStepName)
	if err != nil {
		return errors.Wrap(err, "unable to add end step to drawer")
	}

	return nil
}

func (pd *pipelineDrawer) Finish(p *Pipeline) error {
	if p.measure != nil {
		err := pd.SetTotalTime(endStepName, p.startTime)
		if err != nil {
			return errors.Wrap(err, "unable to set total time")
		}
		err = pd.AddMeasure(p.measure)
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

func PipelineDrawer(drawer drawer.Drawer) PipelineOption {
	return &pipelineDrawer{drawer}
}

type pipelineMeasure struct {
	measure.Measure
}

func (pm *pipelineMeasure) Init(p *Pipeline) error {
	p.measure = pm
	p.measure.AddMetric(startStepName, 1)
	p.measure.AddMetric(endStepName, 1)
	return nil
}

func (pm *pipelineMeasure) Finish(p *Pipeline) error {
	return nil
}

func PipelineMeasure(measure measure.Measure) PipelineOption {
	return &pipelineMeasure{measure}
}

type StepOption[O any] func(s *Step[O])

func StepConcurrency[O any](concurrent int) StepOption[O] {
	return func(s *Step[O]) {
		s.concurrent = concurrent
	}
}

func StepKeepOpen[O any]() StepOption[O] {
	return func(s *Step[O]) {
		s.noClose = true
	}
}

type SplitterOption[I any] func(s *Splitter[I])

func SplitterBufferSize[I any](bufferSize int) SplitterOption[I] {
	return func(s *Splitter[I]) {
		s.bufferSize = bufferSize
	}
}
