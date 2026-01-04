package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

type dropObserver struct {
	PipelineDefaults

	err   error
	calls int
	kind  model.StepDropKind
}

func (o *dropObserver) OnStepDrop(_ *StepInfo, kind model.StepDropKind) error {
	o.calls++
	o.kind = kind

	return o.err
}

type errorRouteObserver struct {
	PipelineDefaults

	err   error
	calls int
}

func (o *errorRouteObserver) OnStepErrorRoute(_ *StepInfo) error {
	o.calls++

	return o.err
}

type prepareStepObserver struct {
	PipelineDefaults

	err error
}

func (o *prepareStepObserver) PrepareStep(_, _ *StepInfo) error {
	return o.err
}

func TestPrepareStepErrorOutput(t *testing.T) {
	t.Parallel()

	step := &Step[int]{ErrorOutputEnabled: true, ErrorOutputBufferSize: 2}
	prepareStepErrorOutput(step)
	require.NotNil(t, step.ErrorOutput)

	original := step.ErrorOutput
	prepareStepErrorOutput(step)
	assert.Equal(t, original, step.ErrorOutput)
}

func TestPrepareErrorStepNoDetails(t *testing.T) {
	t.Parallel()

	pipe := &Pipeline{}
	step := &Step[int]{ErrorStep: &Step[model.StepError]{}}
	require.NoError(t, prepareErrorStep(pipe, step))
}

func TestPrepareErrorStepReportsError(t *testing.T) {
	t.Parallel()

	observer := &prepareStepObserver{err: errors.New("prepare failed")}
	pipe := &Pipeline{opts: []model.PipelineOption{observer}}
	step := &Step[int]{
		Details: &StepInfo{Name: "parent"},
		ErrorStep: &Step[model.StepError]{
			Details: &StepInfo{Name: "error"},
		},
	}

	err := prepareErrorStep(pipe, step)
	require.Error(t, err)
}

func TestReportStepDrop(t *testing.T) {
	t.Parallel()

	require.NoError(t, reportStepDrop([]model.PipelineOption{PipelineDefaults{}}, nil, model.StepDropBufferFull))
	require.NoError(t, reportStepDrop([]model.PipelineOption{PipelineDefaults{}}, &StepInfo{Name: "step"}, model.StepDropBufferFull))

	observer := &dropObserver{}
	require.NoError(t, reportStepDrop([]model.PipelineOption{observer}, &StepInfo{Name: "step"}, model.StepDropBufferFull))
	assert.Equal(t, 1, observer.calls)
	assert.Equal(t, model.StepDropBufferFull, observer.kind)

	err := reportStepDrop([]model.PipelineOption{&dropObserver{err: errors.New("drop failed")}}, &StepInfo{Name: "step"}, model.StepDropBufferFull)
	require.Error(t, err)
}

func TestReportStepErrorRoute(t *testing.T) {
	t.Parallel()

	require.NoError(t, reportStepErrorRoute([]model.PipelineOption{PipelineDefaults{}}, nil))
	require.NoError(t, reportStepErrorRoute([]model.PipelineOption{PipelineDefaults{}}, &StepInfo{Name: "step"}))

	observer := &errorRouteObserver{}
	require.NoError(t, reportStepErrorRoute([]model.PipelineOption{observer}, &StepInfo{Name: "step"}))
	assert.Equal(t, 1, observer.calls)

	err := reportStepErrorRoute([]model.PipelineOption{&errorRouteObserver{err: errors.New("route failed")}}, &StepInfo{Name: "step"})
	require.Error(t, err)
}

func TestRouteStepError(t *testing.T) {
	t.Parallel()

	step := &Step[int]{Details: &StepInfo{Name: "step"}}
	step.ErrorOutputEnabled = true
	step.ErrorOutputBufferSize = 1
	prepareStepErrorOutput(step)

	//nolint:staticcheck // validate nil context is rejected.
	err := routeStepError[int](nil, step, 1, errors.New("boom"), false)
	require.ErrorIs(t, err, ErrContextMustBeSet)

	ctx := context.Background()

	err = routeStepError(ctx, step, 1, nil, false)
	require.NoError(t, err)

	err = routeStepError(ctx, step, 1, errors.New("boom"), false)
	require.NoError(t, err)

	first := <-step.ErrorOutput

	assert.Equal(t, "step", first.StepName)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	step = &Step[int]{Details: &StepInfo{Name: "step"}}
	step.ErrorOutputEnabled = true
	step.ErrorOutputBufferSize = 1
	prepareStepErrorOutput(step)

	observer := &errorRouteObserver{err: errors.New("route failed")}
	err = routeStepError(ctx, step, 1, errors.New("boom"), true, observer)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	step = &Step[int]{Details: &StepInfo{Name: "step"}}
	step.ErrorOutputEnabled = true
	step.ErrorOutputBufferSize = 2
	prepareStepErrorOutput(step)

	observer = &errorRouteObserver{err: errors.New("route failed")}
	err = routeStepError(context.Background(), step, 1, errors.New("boom"), true, observer)
	require.ErrorIs(t, err, observer.err)

	first = <-step.ErrorOutput
	second := <-step.ErrorOutput

	assert.Equal(t, "step", first.StepName)
	assert.Equal(t, "step", second.StepName)
	assert.ErrorIs(t, second.Err, observer.err)
}

func TestSendStepErrorContextCanceled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := sendStepError(ctx, make(chan model.StepError), model.StepError{})
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

func TestSendStepErrorContextDoneDuringRoute(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	step := &Step[int]{
		Details:     &StepInfo{Name: "step"},
		ErrorOutput: make(chan model.StepError),
	}

	err := routeStepError(ctx, step, 1, errors.New("boom"), false)
	require.ErrorIs(t, err, context.Canceled)
}

func TestSendOutputWithPolicyDropOnFull(t *testing.T) {
	t.Parallel()

	step := &Step[int]{
		Details: &StepInfo{Name: "drop-full"},
		Output:  make(chan int),
	}
	step.DropOnOutputFull = true

	observer := &dropObserver{}
	dropped, err := sendOutputWithPolicy(context.Background(), 1, step, 1, nil, true, observer)
	require.NoError(t, err)
	assert.True(t, dropped)
	assert.Equal(t, 1, observer.calls)
	assert.Equal(t, model.StepDropBufferFull, observer.kind)

	dropped, err = sendOutputWithPolicy(context.Background(), 1, step, 1, nil, false)
	require.NoError(t, err)
	assert.True(t, dropped)

	step.Output = make(chan int, 1)
	dropped, err = sendOutputWithPolicy(context.Background(), 1, step, 2, nil, true, observer)
	require.NoError(t, err)
	assert.False(t, dropped)
	assert.Equal(t, 2, <-step.Output)
}

func TestSendOutputWithPolicyDropOnTimeout(t *testing.T) {
	t.Parallel()

	step := &Step[int]{
		Details: &StepInfo{Name: "drop-timeout"},
		Output:  make(chan int),
	}
	step.DropOnOutputTimeout = 5 * time.Millisecond

	observer := &dropObserver{}
	dropped, err := sendOutputWithPolicy(context.Background(), 1, step, 1, nil, true, observer)
	require.NoError(t, err)
	assert.True(t, dropped)
	assert.Equal(t, model.StepDropSendTimeout, observer.kind)
}

func TestSendOutputWithPolicyDropOnTimeoutBranches(t *testing.T) {
	t.Parallel()

	step := &Step[int]{
		Details: &StepInfo{Name: "drop-timeout"},
		Output:  make(chan int, 1),
	}
	step.DropOnOutputTimeout = 5 * time.Millisecond

	timer := time.NewTimer(10 * time.Millisecond)
	defer stopBatchTimer(timer)

	dropped, err := sendOutputWithPolicy(context.Background(), 1, step, 1, timer, false)
	require.NoError(t, err)
	assert.False(t, dropped)
	assert.Equal(t, 1, <-step.Output)

	step.Output = make(chan int)
	dropped, err = sendOutputWithPolicy(context.Background(), 1, step, 2, nil, false)
	require.NoError(t, err)
	assert.True(t, dropped)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	dropped, err = sendOutputWithPolicy(ctx, 1, step, 2, timer, false)
	require.Error(t, err)
	assert.False(t, dropped)
}

func TestSendOutputWithPolicyContextCanceled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	step := &Step[int]{
		Details: &StepInfo{Name: "drop-none"},
		Output:  make(chan int),
	}

	dropped, err := sendOutputWithPolicy(ctx, 1, step, 1, nil, false)
	require.Error(t, err)
	assert.False(t, dropped)
	require.ErrorIs(t, err, context.Canceled)
}

func TestSendOutputWithPolicyNilStep(t *testing.T) {
	t.Parallel()

	dropped, err := sendOutputWithPolicy[int](context.Background(), 1, nil, 1, nil, false)
	require.NoError(t, err)
	assert.False(t, dropped)
}

func TestSendOutputWithPolicyDropOnFullContextDone(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	step := &Step[int]{
		Details: &StepInfo{Name: "drop-full"},
		Output:  make(chan int),
	}
	step.DropOnOutputFull = true

	dropped, err := sendOutputWithPolicy(ctx, 1, step, 1, nil, false)
	require.Error(t, err)
	assert.False(t, dropped)
	require.ErrorIs(t, err, context.Canceled)
}
