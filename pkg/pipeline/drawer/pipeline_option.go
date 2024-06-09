package drawer

import (
	"time"

	"github.com/pkg/errors"

	"github.com/askiada/go-pipeline/pkg/pipeline/measure"
	"github.com/askiada/go-pipeline/pkg/pipeline/model"
)

type pipelineDrawer struct {
	Drawer
	m         measure.Measure
	startTime time.Time
}

// New creates a new pipeline drawer.
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

// PrepareStep is called before the step is executed.
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

// PrepareSplitter is called before the splitter step is executed.
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

// PrepareMerger is called before the merger step is executed.
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

// PrepareSink is called before the sink step is executed.
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

// Finish is called after the pipeline is finished.
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

// OnStepOutput is called after the step output is processed.
func (pd *pipelineDrawer) OnStepOutput(_, _ *model.StepInfo, _, _ time.Duration) error {
	return nil
}

// OnSplitterOutput is called after the splitter step output is processed.
func (pd *pipelineDrawer) OnSplitterOutput(_, _ *model.StepInfo, _, _ time.Duration) error {
	return nil
}

// OnMergerOutput is called after the merger step output is processed.
func (pd *pipelineDrawer) OnMergerOutput(_, _ *model.StepInfo, _ time.Duration) error {
	return nil
}

// OnSinkOutput is called after the sink step output is processed.
func (pd *pipelineDrawer) OnSinkOutput(_, _ *model.StepInfo, _, _ time.Duration) error {
	return nil
}

// AfterSink is called after the sink step is executed.
func (pd *pipelineDrawer) AfterSink(_ *model.StepInfo, _ time.Duration) error {
	return nil
}

// PipelineDrawer creates a pipeline drawer option.
func PipelineDrawer(drw Drawer, msr measure.Measure) model.PipelineOption { //nolint:ireturn // it must implement the interface
	return &pipelineDrawer{drw, msr, time.Now()}
}
