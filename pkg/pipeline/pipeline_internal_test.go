package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

type finishErrOption struct {
	PipelineDefaults

	err error
}

func (o finishErrOption) Finish() error {
	return o.err
}

type errNewOption struct {
	PipelineDefaults

	err error
}

func (o errNewOption) New() error {
	return o.err
}

type pipelineCapsOption struct {
	PipelineDefaults
}

func (pipelineCapsOption) OnStepRetry(_, _ *StepInfo, _ int, _ time.Duration) error {
	return nil
}

func (pipelineCapsOption) OnStepDrop(_ *StepInfo, _ model.StepDropKind) error {
	return nil
}

func (pipelineCapsOption) OnStepErrorRoute(_ *StepInfo) error {
	return nil
}

func (pipelineCapsOption) OnStepOutputMetrics(_, _ *StepInfo, _, _ time.Duration) error {
	return nil
}

func (pipelineCapsOption) OnSplitterOutputMetrics(_, _ *StepInfo, _, _ time.Duration) error {
	return nil
}

func (pipelineCapsOption) OnMergerOutputMetrics(_, _ *StepInfo, _ time.Duration) error {
	return nil
}

func (pipelineCapsOption) OnSinkOutputMetrics(_, _ *StepInfo, _, _ time.Duration) error {
	return nil
}

func (pipelineCapsOption) AfterSinkMetrics(_ *StepInfo, _ time.Duration) error {
	return nil
}

func TestApplyPipelineDefaultsNilPointerSkips(t *testing.T) {
	t.Parallel()

	pipe := &Pipeline{}
	var defaults *PipelineDefaults

	isDefaults, skip := applyPipelineDefaults(pipe, defaults)
	require.False(t, isDefaults)
	require.True(t, skip)
}

func TestApplyPipelineDefaultsSetsValue(t *testing.T) {
	t.Parallel()

	pipe := &Pipeline{}
	defaults := PipelineDefaults{StepBufferSize: 7}

	isDefaults, skip := applyPipelineDefaults(pipe, defaults)
	require.True(t, isDefaults)
	require.False(t, skip)
	require.Equal(t, defaults, pipe.defaults)
}

func TestApplyPipelineDefaultsPointerValue(t *testing.T) {
	t.Parallel()

	pipe := &Pipeline{}
	defaults := &PipelineDefaults{StepConcurrency: 4}

	isDefaults, skip := applyPipelineDefaults(pipe, defaults)
	require.True(t, isDefaults)
	require.False(t, skip)
	require.Equal(t, *defaults, pipe.defaults)
}

func TestCollectPipelineOptionsCapturesCapabilities(t *testing.T) {
	t.Parallel()

	pipe := &Pipeline{}
	var defaultsNil *PipelineDefaults
	defaultsValue := PipelineDefaults{StepBufferSize: 5}
	defaultsPtr := &PipelineDefaults{StepConcurrency: 2}

	opts, metricsOpts, caps, err := collectPipelineOptions(pipe, []model.PipelineOption{
		nil,
		defaultsNil,
		defaultsValue,
		defaultsPtr,
		pipelineCapsOption{},
	})
	require.NoError(t, err)
	require.Len(t, opts, 1)
	require.Len(t, metricsOpts, 1)
	require.True(t, caps.outputMetricsEnabled)
	require.True(t, caps.dropEnabled)
	require.True(t, caps.errorRouteEnabled)
	require.True(t, caps.retryEnabled)
	require.Equal(t, *defaultsPtr, pipe.defaults)
}

func TestPipelineAddRunnerGuards(t *testing.T) {
	t.Parallel()

	var pipe *Pipeline
	pipe.addRunner(func(context.Context) {})

	pipe = &Pipeline{}
	pipe.addRunner(nil)
	require.Empty(t, pipe.runnersSnapshot())
}

func TestPipelineFinishRunPropagatesOptionError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("finish failed")
	pipe, err := New(finishErrOption{err: expectedErr})
	require.NoError(t, err)

	err = pipe.finishRun()
	require.ErrorIs(t, err, expectedErr)
}

func TestPipelineNewPropagatesOptionError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("new failed")
	pipe, err := New(errNewOption{err: expectedErr})
	require.Nil(t, pipe)
	require.ErrorIs(t, err, expectedErr)
}

func TestPipelineErrAndRecordErr(t *testing.T) {
	t.Parallel()

	var pipe *Pipeline
	require.ErrorIs(t, pipe.Err(), ErrPipelineMustBeSet)
	require.Nil(t, pipe.runnersSnapshot())

	pipe = &Pipeline{}
	firstErr := errors.New("first")
	pipe.recordErr("step-a", firstErr)
	require.Error(t, pipe.Err())
	require.ErrorIs(t, pipe.Err(), firstErr)

	pipe.recordErr("step-b", errors.New("second"))
	require.ErrorIs(t, pipe.Err(), firstErr)
}

func TestRateLimiterWaitCanceled(t *testing.T) {
	t.Parallel()

	rl := newRateLimiter(&model.RateLimitPolicy{Every: 50 * time.Millisecond, Burst: 1})
	require.NotNil(t, rl)

	_ = rl.takeToken()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := rl.wait(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRateLimiterTakeTokenNoEvery(t *testing.T) {
	t.Parallel()

	rl := &rateLimiter{}
	require.Equal(t, time.Duration(0), rl.takeToken())
}

func TestNoopRelease(t *testing.T) {
	t.Parallel()

	noopRelease()
}

func TestApplyDefaultsNoopOnNil(t *testing.T) {
	t.Parallel()

	applyStepDefaults[int](nil, nil)
	applyStepDefaults(&Pipeline{}, &Step[int]{})

	applySplitterDefaults[int](nil, nil)
	applySplitterDefaults[int](&Pipeline{}, nil)
}

func TestApplyDefaultsApplyValues(t *testing.T) {
	t.Parallel()

	pipe := &Pipeline{
		defaults: PipelineDefaults{
			StepConcurrency:    3,
			StepKeepOpen:       true,
			StepBufferSize:     8,
			SplitterBufferSize: 12,
		},
	}

	step := &Step[int]{Details: &StepInfo{}}
	applyStepDefaults(pipe, step)
	require.Equal(t, 3, step.Details.Concurrent)
	require.Equal(t, 8, step.Details.BufferSize)
	require.True(t, step.KeepOpen)

	splitter := &Splitter[int]{}
	applySplitterDefaults(pipe, splitter)
	require.Equal(t, 12, splitter.bufferSize)
}
