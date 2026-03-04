package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

type mergerPrepareObserver struct {
	PipelineDefaults

	err error
}

func (o mergerPrepareObserver) PrepareMerger(_ []*StepInfo, _ *StepInfo) error {
	return o.err
}

type mergerOutputObserver struct {
	PipelineDefaults

	err error
}

func (o mergerOutputObserver) OnMergerOutput(_, _ *StepInfo) error {
	return o.err
}

type mergerMetricsObserver struct {
	PipelineDefaults

	err error
}

func (o mergerMetricsObserver) OnStepOutputMetrics(_, _ *StepInfo, _ time.Duration, _ time.Duration) error {
	return nil
}

func (o mergerMetricsObserver) OnSplitterOutputMetrics(_, _ *StepInfo, _ time.Duration, _ time.Duration) error {
	return nil
}

func (o mergerMetricsObserver) OnMergerOutputMetrics(_, _ *StepInfo, _ time.Duration) error {
	return o.err
}

func (o mergerMetricsObserver) OnSinkOutputMetrics(_, _ *StepInfo, _ time.Duration, _ time.Duration) error {
	return nil
}

func (o mergerMetricsObserver) AfterSinkMetrics(_ *StepInfo, _ time.Duration) error {
	return nil
}

func TestPrepareMergerReportsError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("prepare failed")
	pipe := &Pipeline{opts: []model.PipelineOption{mergerPrepareObserver{err: expectedErr}}}
	step := &Step[int]{Details: &StepInfo{Name: "input"}}

	_, err := prepareMerger(pipe, make(chan int), "merge", step)
	require.ErrorIs(t, err, expectedErr)
}

func TestReportMergerOutputErrors(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[int]{Details: &StepInfo{Name: "output"}}

	stepErr := errors.New("output hook failed")
	cfg := hookConfig{
		opts: []model.PipelineOption{mergerOutputObserver{err: stepErr}},
	}

	err := reportMergerOutput(cfg, input, output, 0)
	require.ErrorIs(t, err, stepErr)

	metricErr := errors.New("metric hook failed")
	cfg = hookConfig{
		outputMetrics: true,
		metricsOpts:   []model.PipelineMetricsOption{mergerMetricsObserver{err: metricErr}},
	}

	err = reportMergerOutput(cfg, input, output, time.Millisecond)
	require.ErrorIs(t, err, metricErr)
}

func TestRunStepMergerContextDone(t *testing.T) {
	t.Parallel()

	pipe, err := New(PipelineDefaults{})
	require.NoError(t, err)

	step := &Step[int]{Details: &StepInfo{Name: "input"}, Output: make(chan int)}
	output := &Step[int]{Details: &StepInfo{Name: "output"}, Output: make(chan int)}
	errC := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	runStepMerger(ctx, pipe, errC, step, output)

	select {
	case err := <-errC:
		require.ErrorIs(t, err, context.Canceled)
	default:
		t.Fatal("expected cancellation error")
	}
}

func TestRunStepMergerReportsOutputError(t *testing.T) {
	t.Parallel()

	outputErr := errors.New("output hook failed")
	pipe, err := New(mergerOutputObserver{err: outputErr})
	require.NoError(t, err)

	step := &Step[int]{Details: &StepInfo{Name: "input"}, Output: make(chan int, 1)}
	output := &Step[int]{Details: &StepInfo{Name: "output"}, Output: make(chan int, 1)}
	errC := make(chan error, 2)

	step.Output <- 1

	close(step.Output)

	runStepMerger(context.Background(), pipe, errC, step, output)

	select {
	case err := <-errC:
		require.ErrorIs(t, err, outputErr)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected output error")
	}
}

func TestMergeBuildErrors(t *testing.T) {
	t.Parallel()

	require.Nil(t, Merge[int](nil, "merge"))

	pipe := &Pipeline{buildErr: errors.New("build failed")}
	require.Nil(t, Merge[int](pipe, "merge", &Step[int]{}))

	pipe = &Pipeline{}
	require.Nil(t, Merge[int](pipe, "merge"))
	require.ErrorIs(t, pipe.Err(), ErrInputMustBeSet)

	pipe = &Pipeline{}
	require.Nil(t, Merge[int](pipe, "merge", nil))
	require.ErrorIs(t, pipe.Err(), ErrInputMustBeSet)
}

func TestMergePrepareError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("prepare failed")
	pipe, err := New(mergerPrepareObserver{err: expectedErr})
	require.NoError(t, err)

	input := &Step[int]{Details: &StepInfo{Name: "input"}, Output: make(chan int)}
	merge := Merge(pipe, "merge", input)
	require.Nil(t, merge)
	require.ErrorIs(t, pipe.Err(), expectedErr)
}
