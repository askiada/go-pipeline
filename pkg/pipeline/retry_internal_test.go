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

type retryObserver struct {
	PipelineDefaults

	err   error
	calls int
}

func (o *retryObserver) OnStepRetry(_, _ *StepInfo, _ int, _ time.Duration) error {
	o.calls++

	return o.err
}

func TestRandomJitterSeedWithinRange(t *testing.T) {
	t.Parallel()

	seed, err := randomJitterSeed()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, seed, 0.0)
	assert.LessOrEqual(t, seed, 1.0)
}

func TestShouldRetry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil error", func(t *testing.T) {
		t.Parallel()

		assert.False(t, shouldRetry(ctx, &model.RetryPolicy{}, nil))
	})

	t.Run("context canceled", func(t *testing.T) {
		t.Parallel()

		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()

		assert.False(t, shouldRetry(cancelCtx, &model.RetryPolicy{}, errors.New("boom")))
	})

	t.Run("nil policy", func(t *testing.T) {
		t.Parallel()

		assert.False(t, shouldRetry(ctx, nil, errors.New("boom")))
	})

	t.Run("retry on hook", func(t *testing.T) {
		t.Parallel()

		allowed := &model.RetryPolicy{
			RetryOn: func(_ error) bool { return true },
		}
		denied := &model.RetryPolicy{
			RetryOn: func(_ error) bool { return false },
		}

		assert.True(t, shouldRetry(ctx, allowed, errors.New("boom")))
		assert.False(t, shouldRetry(ctx, denied, errors.New("boom")))
	})

	t.Run("cancellation errors do not retry", func(t *testing.T) {
		t.Parallel()

		assert.False(t, shouldRetry(ctx, &model.RetryPolicy{}, context.Canceled))
		assert.False(t, shouldRetry(ctx, &model.RetryPolicy{}, context.DeadlineExceeded))
	})
}

func TestRetryDelay(t *testing.T) {
	t.Parallel()

	assert.Zero(t, retryDelay(nil, 1))
	assert.Zero(t, retryDelay(&model.RetryPolicy{Backoff: 0}, 1))

	policy := &model.RetryPolicy{Backoff: 10 * time.Millisecond}
	assert.Equal(t, 10*time.Millisecond, retryDelay(policy, 0))

	clamped := &model.RetryPolicy{Backoff: 10 * time.Millisecond, MaxBackoff: 15 * time.Millisecond}
	assert.Equal(t, 15*time.Millisecond, retryDelay(clamped, 5))

	jittered := &model.RetryPolicy{Backoff: 10 * time.Millisecond, Jitter: 1}
	delay := retryDelay(jittered, 1)
	assert.GreaterOrEqual(t, delay, time.Duration(0))
	assert.LessOrEqual(t, delay, 20*time.Millisecond)
}

func TestSleepRetry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	require.NoError(t, sleepRetry(ctx, &model.RetryPolicy{Backoff: 0}, 1))

	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()

	err := sleepRetry(cancelCtx, &model.RetryPolicy{Backoff: 5 * time.Millisecond}, 1)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

func TestReportStepRetry(t *testing.T) {
	t.Parallel()

	parent := &StepInfo{Name: "parent"}
	step := &StepInfo{Name: "step"}

	observer := &retryObserver{}
	require.NoError(t, reportStepRetry([]model.PipelineOption{observer}, parent, step, 1, time.Millisecond))
	assert.Equal(t, 1, observer.calls)

	expectedErr := errors.New("retry report failed")
	observer.err = expectedErr
	err := reportStepRetry([]model.PipelineOption{observer}, parent, step, 2, time.Millisecond)
	require.Error(t, err)
	require.ErrorIs(t, err, expectedErr)
}

func TestExecuteWithRetryNoPolicy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	calls := 0

	out, duration, err := executeWithRetry(ctx, nil, func() (int, error) {
		calls++

		return 5, nil
	}, nil, false)
	require.NoError(t, err)
	assert.Equal(t, 5, out)
	assert.Zero(t, duration)
	assert.Equal(t, 1, calls)
}

func TestExecuteWithRetryNoPolicyWithTiming(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	out, duration, err := executeWithRetry(ctx, nil, func() (int, error) {
		return 7, nil
	}, nil, true)
	require.NoError(t, err)
	assert.Equal(t, 7, out)
	assert.GreaterOrEqual(t, duration, time.Duration(0))
}

func TestExecuteWithRetryReportsAndSucceeds(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	policy := &model.RetryPolicy{MaxAttempts: 3}
	attempts := 0
	reports := 0

	out, duration, err := executeWithRetry(ctx, policy, func() (int, error) {
		attempts++
		if attempts < 3 {
			return 0, assert.AnError
		}

		return 42, nil
	}, func(_ int, _ time.Duration) error {
		reports++

		return nil
	}, true)
	require.NoError(t, err)
	assert.Equal(t, 42, out)
	assert.Equal(t, 3, attempts)
	assert.Equal(t, 2, reports)
	assert.GreaterOrEqual(t, duration, time.Duration(0))
}

func TestExecuteWithRetryStopsOnReportError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	policy := &model.RetryPolicy{MaxAttempts: 2}
	reportErr := errors.New("report retry failed")

	_, _, err := executeWithRetry(ctx, policy, func() (int, error) {
		return 0, assert.AnError
	}, func(_ int, _ time.Duration) error {
		return reportErr
	}, false)
	require.Error(t, err)
	require.ErrorIs(t, err, reportErr)
}

func TestExecuteWithRetryStopsOnSleepError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	policy := &model.RetryPolicy{MaxAttempts: 3, Backoff: 50 * time.Millisecond}

	_, _, err := executeWithRetry(ctx, policy, func() (int, error) {
		return 0, assert.AnError
	}, nil, false)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
