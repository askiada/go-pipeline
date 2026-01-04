package pipeline

import (
	"bytes"
	"context"
	"errors"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

type splitterPrepareObserver struct {
	PipelineDefaults

	err error
}

func (o splitterPrepareObserver) PrepareSplitter(_, _ *StepInfo) error {
	return o.err
}

type splitterOutputObserver struct {
	PipelineDefaults

	err error
}

func (o splitterOutputObserver) OnSplitterOutput(_, _ *StepInfo) error {
	return o.err
}

type splitterMetricsObserver struct {
	PipelineDefaults

	err error
}

func (o splitterMetricsObserver) OnStepOutputMetrics(_, _ *StepInfo, _, _ time.Duration) error {
	return nil
}

func (o splitterMetricsObserver) OnSplitterOutputMetrics(_, _ *StepInfo, _, _ time.Duration) error {
	return o.err
}

func (o splitterMetricsObserver) OnMergerOutputMetrics(_, _ *StepInfo, _ time.Duration) error {
	return nil
}

func (o splitterMetricsObserver) OnSinkOutputMetrics(_, _ *StepInfo, _, _ time.Duration) error {
	return nil
}

func (o splitterMetricsObserver) AfterSinkMetrics(_ *StepInfo, _ time.Duration) error {
	return nil
}

func TestPipelineDefaultsApplyToSplitter(t *testing.T) {
	t.Parallel()

	defaults := PipelineDefaults{
		SplitterBufferSize: 7,
	}
	pipe, err := New(defaults)
	require.NoError(t, err)

	input := &Step[int]{
		Details: &model.StepInfo{
			Name:       "input",
			Concurrent: 1,
		},
		Output: make(chan int),
	}
	close(input.Output)

	splitter := Split(pipe, "split", input, 2)
	require.NotNil(t, splitter)
	require.Equal(t, 7, splitter.bufferSize)
}

func TestPipelineDefaultsOverrideSplitterBufferSize(t *testing.T) {
	t.Parallel()

	defaults := PipelineDefaults{
		SplitterBufferSize: 7,
	}
	pipe, err := New(defaults)
	require.NoError(t, err)

	input := &Step[int]{
		Details: &model.StepInfo{
			Name:       "input",
			Concurrent: 1,
		},
		Output: make(chan int),
	}
	close(input.Output)

	splitter := Split(pipe, "split", input, 2, SplitterBufferSize[int](2))
	require.NotNil(t, splitter)
	require.Equal(t, 2, splitter.bufferSize)
}

func TestWarnSplitterBufferSmallLogs(t *testing.T) {
	var buf bytes.Buffer
	origOutput := log.Writer()
	origFlags := log.Flags()

	log.SetOutput(&buf)
	log.SetFlags(0)

	defer func() {
		log.SetOutput(origOutput)
		log.SetFlags(origFlags)
	}()

	warnSplitterBuffer("split", 1, 4)

	require.Contains(t, buf.String(), "smaller than input concurrency")
}

func TestWarnSplitterBufferLargeLogs(t *testing.T) {
	var buf bytes.Buffer
	origOutput := log.Writer()
	origFlags := log.Flags()

	log.SetOutput(&buf)
	log.SetFlags(0)

	defer func() {
		log.SetOutput(origOutput)
		log.SetFlags(origFlags)
	}()

	warnSplitterBuffer("split", 32, 2)

	require.Contains(t, buf.String(), "much larger than input concurrency")
}

func TestSplitterAllowsNilInputDetails(t *testing.T) {
	t.Parallel()

	pipe, err := New(PipelineDefaults{})
	require.NoError(t, err)

	input := &Step[int]{
		Output: make(chan int),
	}
	close(input.Output)

	splitter := Split(pipe, "split", input, 1)
	require.NotNil(t, splitter)
}

func TestSplitterGetReturnsFalseAtEnd(t *testing.T) {
	t.Parallel()

	splitter := &Splitter[int]{
		splittedSteps: []*Step[int]{{Details: &StepInfo{Name: "step"}}},
	}

	step, ok := splitter.Get()
	require.True(t, ok)
	require.Equal(t, "step", step.Details.Name)

	step, ok = splitter.Get()
	require.False(t, ok)
	require.Nil(t, step)
}

func TestPrepareSplitterReportsOptionError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("prepare failed")
	pipe := &Pipeline{opts: []model.PipelineOption{splitterPrepareObserver{err: expectedErr}}}
	input := &Step[int]{Details: &StepInfo{Name: "input"}}

	_, err := prepareSplitter(pipe, "split", input, 1)
	require.ErrorIs(t, err, expectedErr)
}

func TestRunSplitterLoopRouteError(t *testing.T) {
	t.Parallel()

	pipe := &Pipeline{}

	input := &Step[int]{Details: &StepInfo{Name: "input"}, Output: make(chan int, 1)}

	input.Output <- 1

	close(input.Output)

	splitter, err := prepareSplitter(pipe, "split", input, 1)
	require.NoError(t, err)

	errC := make(chan error, 1)
	routeErr := errors.New("route failed")

	runSplitterLoop(context.Background(), pipe, splitter, input, errC, func(context.Context, int, func(int) error) error {
		return routeErr
	})

	err = <-errC
	require.ErrorIs(t, err, routeErr)
}

func TestRunSplitterLoopOutputHookError(t *testing.T) {
	t.Parallel()

	outputErr := errors.New("output hook failed")
	pipe := &Pipeline{opts: []model.PipelineOption{splitterOutputObserver{err: outputErr}}}

	input := &Step[int]{Details: &StepInfo{Name: "input"}, Output: make(chan int, 1)}

	input.Output <- 1

	close(input.Output)

	splitter, err := prepareSplitter(pipe, "split", input, 1)
	require.NoError(t, err)

	errC := make(chan error, 1)

	runSplitterLoop(context.Background(), pipe, splitter, input, errC, func(ctx context.Context, entry int, send func(int) error) error {
		return send(0)
	})

	err = <-errC
	require.ErrorIs(t, err, outputErr)
}

func TestRunSplitterLoopMetricsError(t *testing.T) {
	t.Parallel()

	metricErr := errors.New("metric failed")
	pipe := &Pipeline{
		outputMetricsEnabled: true,
		metricsOpts:          []model.PipelineMetricsOption{splitterMetricsObserver{err: metricErr}},
	}

	input := &Step[int]{Details: &StepInfo{Name: "input"}, Output: make(chan int, 1)}

	input.Output <- 1

	close(input.Output)

	splitter, err := prepareSplitter(pipe, "split", input, 1)
	require.NoError(t, err)

	errC := make(chan error, 1)

	runSplitterLoop(context.Background(), pipe, splitter, input, errC, func(ctx context.Context, entry int, send func(int) error) error {
		return send(0)
	})

	err = <-errC
	require.ErrorIs(t, err, metricErr)
}

func TestRunSplitterLoopContextDone(t *testing.T) {
	t.Parallel()

	pipe := &Pipeline{}
	input := &Step[int]{Details: &StepInfo{Name: "input"}, Output: make(chan int)}

	splitter, err := prepareSplitter(pipe, "split", input, 1)
	require.NoError(t, err)

	errC := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	runSplitterLoop(ctx, pipe, splitter, input, errC, func(context.Context, int, func(int) error) error {
		return nil
	})

	err = <-errC
	require.ErrorIs(t, err, context.Canceled)
}

func TestRunSplitByFunctionError(t *testing.T) {
	t.Parallel()

	pipe := &Pipeline{}

	input := &Step[int]{Details: &StepInfo{Name: "input"}, Output: make(chan int, 1)}

	input.Output <- 1

	close(input.Output)

	splitter, err := prepareSplitter(pipe, "split", input, 1)
	require.NoError(t, err)

	errC := make(chan error, 1)
	splitErr := errors.New("split failed")

	runSplitBy(context.Background(), pipe, splitter, input, errC, []SplitFn[int]{
		func(context.Context, int) (bool, error) {
			return false, splitErr
		},
	})

	err = <-errC
	require.ErrorIs(t, err, splitErr)
}

func TestSplitByBuildErrors(t *testing.T) {
	t.Parallel()

	require.Nil(t, SplitBy[int](nil, "split", nil, nil))

	pipe := &Pipeline{buildErr: errors.New("build failed")}
	require.Nil(t, SplitBy(pipe, "split", &Step[int]{}, []SplitFn[int]{}))
}

func TestSplitByEmptyFnsRecordsError(t *testing.T) {
	t.Parallel()

	pipe, err := New(PipelineDefaults{})
	require.NoError(t, err)

	input := &Step[int]{Details: &StepInfo{Name: "input"}, Output: make(chan int)}
	close(input.Output)

	splitter := SplitBy(pipe, "split", input, nil)
	require.Nil(t, splitter)
	require.ErrorIs(t, pipe.Err(), ErrSplitterTotal)
}
