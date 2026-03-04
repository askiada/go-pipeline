package drawer

import (
	"fmt"
	"time"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline"
	"github.com/askiada/go-pipeline/v2/pkg/pipeline/measure"
	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

type pipelineDrawer struct {
	Drawer

	m         measure.Measure
	runOpts   model.RunOptions
	startTime time.Time
}

// New creates a new pipeline drawer.
func (pd *pipelineDrawer) New() error {
	err := pd.AddStep(model.StartStep.Details.Name)
	if err != nil {
		return fmt.Errorf("unable to add start step to drawer: %w", err)
	}

	err = pd.AddStep(model.EndStep.Details.Name)
	if err != nil {
		return fmt.Errorf("unable to add end step to drawer: %w", err)
	}

	return nil
}

// PrepareStep is called before the step is executed.
func (pd *pipelineDrawer) PrepareStep(parentStep, step *pipeline.StepInfo) error {
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
func (pd *pipelineDrawer) PrepareSplitter(parentStep, splitterStep *pipeline.StepInfo) error {
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
func (pd *pipelineDrawer) PrepareMerger(parentStep []*pipeline.StepInfo, step *pipeline.StepInfo) error {
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
func (pd *pipelineDrawer) PrepareSink(parentStep, step *pipeline.StepInfo) error {
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
	if pd.m != nil && !pd.runOpts.DryRun {
		err := pd.SetTotalTime(model.EndStep.Details.Name, pd.startTime)
		if err != nil {
			return fmt.Errorf("unable to set total time: %w", err)
		}

		err = pd.AddMeasure(pd.m)
		if err != nil {
			return fmt.Errorf("unable to add measure: %w", err)
		}
	}

	err := pd.Draw()
	if err != nil {
		return fmt.Errorf("unable to draw pipeline: %w", err)
	}

	return nil
}

// OnStepOutput is called after the step output is processed.
func (pd *pipelineDrawer) OnStepOutput(_, _ *pipeline.StepInfo) error {
	return nil
}

// OnSplitterOutput is called after the splitter step output is processed.
func (pd *pipelineDrawer) OnSplitterOutput(_, _ *pipeline.StepInfo) error {
	return nil
}

// OnMergerOutput is called after the merger step output is processed.
func (pd *pipelineDrawer) OnMergerOutput(_, _ *pipeline.StepInfo) error {
	return nil
}

// OnSinkOutput is called after the sink step output is processed.
func (pd *pipelineDrawer) OnSinkOutput(_, _ *pipeline.StepInfo) error {
	return nil
}

// AfterSink is called after the sink step is executed.
func (pd *pipelineDrawer) AfterSink(_ *pipeline.StepInfo) error {
	return nil
}

// SetRunOptions records the run settings so Finish can honour dry-run behaviour.
func (pd *pipelineDrawer) SetRunOptions(opts model.RunOptions) {
	pd.runOpts = opts
}

// PipelineDrawer returns a pipeline option that draws a graph with the Drawer.
// When a Measure is provided, it adds metrics after a real run (not dry-run).
//
//nolint:ireturn // Public API returns the option interface.
func PipelineDrawer(drw Drawer, msr measure.Measure) model.PipelineOption {
	return &pipelineDrawer{
		Drawer:    drw,
		m:         msr,
		startTime: time.Now(),
	}
}
