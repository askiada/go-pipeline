package pipeline

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStepConcurrencyClamps(t *testing.T) {
	t.Parallel()

	step := &Step[int]{Details: &StepInfo{}}
	StepConcurrency[int](0)(step)
	assert.Equal(t, 1, step.Details.Concurrent)
}

func TestStepRetryClampsAndSkips(t *testing.T) {
	t.Parallel()

	step := &Step[int]{Details: &StepInfo{}}
	StepRetry[int](RetryPolicy{MaxAttempts: 1})(step)
	assert.Nil(t, step.RetryPolicy)

	step = &Step[int]{Details: &StepInfo{}}
	StepRetry[int](RetryPolicy{
		MaxAttempts: 2,
		Backoff:     -1,
		MaxBackoff:  -2,
		Jitter:      -0.5,
	})(step)
	require.NotNil(t, step.RetryPolicy)
	assert.Equal(t, time.Duration(0), step.RetryPolicy.Backoff)
	assert.Equal(t, time.Duration(0), step.RetryPolicy.MaxBackoff)
	assert.InDelta(t, 0.0, step.RetryPolicy.Jitter, 1e-9)

	step = &Step[int]{Details: &StepInfo{}}
	StepRetry[int](RetryPolicy{
		MaxAttempts: 2,
		Jitter:      2,
	})(step)
	require.NotNil(t, step.RetryPolicy)
	assert.InDelta(t, 1.0, step.RetryPolicy.Jitter, 1e-9)
}

func TestStepTimeoutSkipsNonPositive(t *testing.T) {
	t.Parallel()

	step := &Step[int]{Details: &StepInfo{}}
	StepTimeout[int](0)(step)
	assert.Zero(t, step.Timeout)

	StepTimeout[int](10 * time.Millisecond)(step)
	assert.Equal(t, 10*time.Millisecond, step.Timeout)
}

func TestStepRateLimitSkipsInvalid(t *testing.T) {
	t.Parallel()

	step := &Step[int]{Details: &StepInfo{}}
	StepRateLimit[int](RateLimitPolicy{Every: 0})(step)
	assert.Nil(t, step.RateLimitPolicy)

	StepRateLimit[int](RateLimitPolicy{Every: time.Millisecond, Burst: 0})(step)
	require.NotNil(t, step.RateLimitPolicy)
	assert.Equal(t, 1, step.RateLimitPolicy.Burst)
}

func TestStepMaxInFlightSkipsInvalid(t *testing.T) {
	t.Parallel()

	step := &Step[int]{Details: &StepInfo{}}
	StepMaxInFlight[int](0)(step)
	assert.Zero(t, step.MaxInFlight)

	StepMaxInFlight[int](2)(step)
	assert.Equal(t, 2, step.MaxInFlight)
}

func TestStepDropOnBlockedSkipsInvalid(t *testing.T) {
	t.Parallel()

	step := &Step[int]{Details: &StepInfo{}}
	StepDropOnBlocked[int](0)(step)
	assert.Zero(t, step.DropOnOutputTimeout)

	StepDropOnBlocked[int](5 * time.Millisecond)(step)
	assert.Equal(t, 5*time.Millisecond, step.DropOnOutputTimeout)
}

func TestStepErrorOutputOption(t *testing.T) {
	t.Parallel()

	errorStep, opt := StepErrorOutput[int](-1)
	assert.Equal(t, 0, errorStep.Details.BufferSize)

	step := &Step[int]{Details: &StepInfo{Name: "source"}}
	opt(step)
	require.NotNil(t, step.ErrorOutput)
	assert.Equal(t, "source error", errorStep.Details.Name)

	opt(nil)
}
