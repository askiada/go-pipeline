package drawer_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/drawer"
	"github.com/askiada/go-pipeline/v2/pkg/pipeline/measure"
	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

type stubDrawer struct {
	calls         []string
	addStepErr    error
	addLinkErr    error
	setTotalErr   error
	addMeasureErr error
	drawErr       error
}

func (s *stubDrawer) AddStep(stepName string) error {
	s.calls = append(s.calls, "AddStep:"+stepName)

	if s.addStepErr != nil {
		return s.addStepErr
	}

	return nil
}

func (s *stubDrawer) AddLink(parentStepName, childrenStepName string) error {
	s.calls = append(s.calls, "AddLink:"+parentStepName+"->"+childrenStepName)

	if s.addLinkErr != nil {
		return s.addLinkErr
	}

	return nil
}

func (s *stubDrawer) Draw() error {
	s.calls = append(s.calls, "Draw")

	return s.drawErr
}

func (s *stubDrawer) SetTotalTime(stepName string, _ time.Time) error {
	s.calls = append(s.calls, "SetTotalTime:"+stepName)

	return s.setTotalErr
}

func (s *stubDrawer) AddMeasure(_ measure.Measure) error {
	s.calls = append(s.calls, "AddMeasure")

	return s.addMeasureErr
}

func TestPipelineDrawerNewAddsStartAndEnd(t *testing.T) {
	t.Parallel()

	stub := &stubDrawer{}
	opt := drawer.PipelineDrawer(stub, nil)

	require.NoError(t, opt.New())

	require.Equal(t, []string{
		"AddStep:" + model.StartStep.Details.Name,
		"AddStep:" + model.EndStep.Details.Name,
	}, stub.calls)
}

func TestPipelineDrawerNewPropagatesAddStepError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("add step failed")
	stub := &stubDrawer{addStepErr: expectedErr}
	opt := drawer.PipelineDrawer(stub, nil)

	require.ErrorIs(t, opt.New(), expectedErr)
}

type secondFailDrawer struct {
	calls int
	err   error
}

func (s *secondFailDrawer) AddStep(string) error {
	s.calls++
	if s.calls == 2 {
		return s.err
	}

	return nil
}

func (s *secondFailDrawer) AddLink(string, string) error {
	return nil
}

func (s *secondFailDrawer) Draw() error {
	return nil
}

func (s *secondFailDrawer) SetTotalTime(string, time.Time) error {
	return nil
}

func (s *secondFailDrawer) AddMeasure(measure.Measure) error {
	return nil
}

func TestPipelineDrawerNewSecondAddStepError(t *testing.T) {
	t.Parallel()

	fail := errors.New("second add failed")
	drw := &secondFailDrawer{err: fail}
	opt := drawer.PipelineDrawer(drw, nil)

	require.ErrorIs(t, opt.New(), fail)
}

func TestPipelineDrawerPrepareHooks(t *testing.T) {
	t.Parallel()

	stub := &stubDrawer{}
	opt := drawer.PipelineDrawer(stub, nil)

	parent := &model.StepInfo{Name: "parent", Concurrent: 1}
	step := &model.StepInfo{Name: "step", Concurrent: 1}
	merger := &model.StepInfo{Name: "merger", Concurrent: 1}

	require.NoError(t, opt.PrepareStep(parent, step))
	require.NoError(t, opt.PrepareSplitter(parent, step))
	require.NoError(t, opt.PrepareMerger([]*model.StepInfo{parent, step}, merger))
	require.NoError(t, opt.PrepareSink(parent, step))
}

func TestPipelineDrawerPrepareStepErrors(t *testing.T) {
	t.Parallel()

	stepErr := errors.New("add step failed")
	linkErr := errors.New("add link failed")

	parent := &model.StepInfo{Name: "parent", Concurrent: 1}
	step := &model.StepInfo{Name: "step", Concurrent: 1}

	stub := &stubDrawer{addStepErr: stepErr}
	opt := drawer.PipelineDrawer(stub, nil)
	require.ErrorIs(t, opt.PrepareStep(parent, step), stepErr)

	stub = &stubDrawer{addLinkErr: linkErr}
	opt = drawer.PipelineDrawer(stub, nil)
	require.ErrorIs(t, opt.PrepareStep(parent, step), linkErr)
}

func TestPipelineDrawerPrepareSplitterErrors(t *testing.T) {
	t.Parallel()

	stepErr := errors.New("add step failed")
	linkErr := errors.New("add link failed")

	parent := &model.StepInfo{Name: "parent", Concurrent: 1}
	step := &model.StepInfo{Name: "splitter", Concurrent: 1}

	stub := &stubDrawer{addStepErr: stepErr}
	opt := drawer.PipelineDrawer(stub, nil)
	require.ErrorIs(t, opt.PrepareSplitter(parent, step), stepErr)

	stub = &stubDrawer{addLinkErr: linkErr}
	opt = drawer.PipelineDrawer(stub, nil)
	require.ErrorIs(t, opt.PrepareSplitter(parent, step), linkErr)
}

func TestPipelineDrawerPrepareMergerErrors(t *testing.T) {
	t.Parallel()

	stepErr := errors.New("add step failed")
	linkErr := errors.New("add link failed")

	parent := &model.StepInfo{Name: "parent", Concurrent: 1}
	step := &model.StepInfo{Name: "merger", Concurrent: 1}

	stub := &stubDrawer{addStepErr: stepErr}
	opt := drawer.PipelineDrawer(stub, nil)
	require.ErrorIs(t, opt.PrepareMerger([]*model.StepInfo{parent}, step), stepErr)

	stub = &stubDrawer{addLinkErr: linkErr}
	opt = drawer.PipelineDrawer(stub, nil)
	require.ErrorIs(t, opt.PrepareMerger([]*model.StepInfo{parent}, step), linkErr)
}

func TestPipelineDrawerPrepareSinkErrors(t *testing.T) {
	t.Parallel()

	stepErr := errors.New("add step failed")
	linkErr := errors.New("add link failed")

	parent := &model.StepInfo{Name: "parent", Concurrent: 1}
	step := &model.StepInfo{Name: "sink", Concurrent: 1}

	stub := &stubDrawer{addStepErr: stepErr}
	opt := drawer.PipelineDrawer(stub, nil)
	require.ErrorIs(t, opt.PrepareSink(parent, step), stepErr)

	stub = &stubDrawer{addLinkErr: linkErr}
	opt = drawer.PipelineDrawer(stub, nil)
	require.ErrorIs(t, opt.PrepareSink(parent, step), linkErr)
}

func TestPipelineDrawerFinishWithMeasure(t *testing.T) {
	t.Parallel()

	stub := &stubDrawer{}
	msr := measure.NewDefaultMeasure()
	opt := drawer.PipelineDrawer(stub, msr)

	require.NoError(t, opt.Finish())
	require.Equal(t, []string{
		"SetTotalTime:" + model.EndStep.Details.Name,
		"AddMeasure",
		"Draw",
	}, stub.calls)
}

func TestPipelineDrawerFinishDryRunOmitsMeasure(t *testing.T) {
	t.Parallel()

	stub := &stubDrawer{}
	msr := measure.NewDefaultMeasure()
	opt := drawer.PipelineDrawer(stub, msr)

	runAware, ok := opt.(model.RunOptionAware)
	require.True(t, ok)

	runAware.SetRunOptions(model.RunOptions{DryRun: true})

	require.NoError(t, opt.Finish())
	require.Equal(t, []string{
		"Draw",
	}, stub.calls)
}

func TestPipelineDrawerFinishPropagatesErrors(t *testing.T) {
	t.Parallel()

	setTotalErr := errors.New("set total failed")
	addMeasureErr := errors.New("add measure failed")
	drawErr := errors.New("draw failed")

	stub := &stubDrawer{setTotalErr: setTotalErr}
	msr := measure.NewDefaultMeasure()
	opt := drawer.PipelineDrawer(stub, msr)
	require.ErrorIs(t, opt.Finish(), setTotalErr)

	stub = &stubDrawer{addMeasureErr: addMeasureErr}
	opt = drawer.PipelineDrawer(stub, msr)
	require.ErrorIs(t, opt.Finish(), addMeasureErr)

	stub = &stubDrawer{drawErr: drawErr}
	opt = drawer.PipelineDrawer(stub, nil)
	require.ErrorIs(t, opt.Finish(), drawErr)
}

func TestPipelineDrawerOutputHooksNoop(t *testing.T) {
	t.Parallel()

	stub := &stubDrawer{}
	opt := drawer.PipelineDrawer(stub, nil)

	parent := &model.StepInfo{Name: "parent", Concurrent: 1}
	step := &model.StepInfo{Name: "step", Concurrent: 1}

	require.NoError(t, opt.OnStepOutput(parent, step))
	require.NoError(t, opt.OnSplitterOutput(parent, step))
	require.NoError(t, opt.OnMergerOutput(parent, step))
	require.NoError(t, opt.OnSinkOutput(parent, step))
	require.NoError(t, opt.AfterSink(step))
}
