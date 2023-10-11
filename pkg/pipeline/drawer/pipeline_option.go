package drawer

import (
	"time"

	"github.com/askiada/go-pipeline/pkg/pipeline/measure"
	"github.com/askiada/go-pipeline/pkg/pipeline/model"
	"github.com/pkg/errors"
)

type pipelineDrawer struct {
	Drawer
	m         measure.Measure
	startTime time.Time
}

func (pd *pipelineDrawer) New() error {
	err := pd.AddStep(model.StartStep.Details.Name)
	if err != nil {
		return errors.Wrap(err, "unable to add start step to drawer")
	}
	err = pd.AddStep(model.EndStep.Details.Name)
	if err != nil {
		return errors.Wrap(err, "unable to add end step to drawer")
	}

	return nil
}

func (pd *pipelineDrawer) PrepareStep(parentStep, step *model.StepInfo) error {
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

func (pd *pipelineDrawer) PrepareSplitter(parentStep, splitterStep *model.StepInfo) error {
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

func (pd *pipelineDrawer) PrepareMerger(parentStep []*model.StepInfo, step *model.StepInfo) error {
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

func (pd *pipelineDrawer) PrepareSink(parentStep, step *model.StepInfo) error {
	err := pd.AddStep(step.Name)
	if err != nil {
		return err
	}
	err = pd.AddLink(parentStep.Name, step.Name)
	if err != nil {
		return err
	}
	err = pd.AddLink(step.Name, model.EndStep.Details.Name)
	if err != nil {
		return err
	}

	return nil
}

func (pd *pipelineDrawer) Finish() error {
	if pd.m != nil {
		err := pd.SetTotalTime(model.EndStep.Details.Name, pd.startTime)
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

func (pd *pipelineDrawer) OnStepOutput(parentStep, step *model.StepInfo, iterationDuration, computationDuration time.Duration) error {
	return nil
}

func (pd *pipelineDrawer) OnSplitterOutput(parentStep, step *model.StepInfo, iterationDuration, computationDuration time.Duration) error {
	return nil
}

func (pd *pipelineDrawer) OnMergerOutput(parentStep *model.StepInfo, outputStep *model.StepInfo, iterationDuration time.Duration) error {
	return nil
}

func (pd *pipelineDrawer) OnSinkOutput(parentStep, step *model.StepInfo, iterationDuration, computationDuration time.Duration) error {
	return nil
}

func (pd *pipelineDrawer) AfterSink(step *model.StepInfo, totalDuration time.Duration) error {
	return nil
}

func PipelineDrawer(drawer Drawer, measure measure.Measure) model.PipelineOption {
	return &pipelineDrawer{drawer, measure, time.Now()}
}
