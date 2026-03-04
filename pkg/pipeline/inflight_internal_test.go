package pipeline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInFlightLimiterAcquireRelease(t *testing.T) {
	t.Parallel()

	require.Nil(t, newInFlightLimiter(0))

	limiter := newInFlightLimiter(1)
	require.NotNil(t, limiter)

	ctx := context.Background()
	require.NoError(t, limiter.acquire(ctx))
	limiter.release()
	limiter.release()
}

func TestInFlightLimiterAcquireContextCancel(t *testing.T) {
	t.Parallel()

	limiter := newInFlightLimiter(1)
	require.NoError(t, limiter.acquire(context.Background()))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := limiter.acquire(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	limiter.release()
}

func TestInFlightLimiterNilNoop(t *testing.T) {
	t.Parallel()

	var limiter *inFlightLimiter
	require.NoError(t, limiter.acquire(context.Background()))
	limiter.release()
}
