package pipeline

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"time"

	"github.com/pkg/errors"

	"github.com/askiada/go-pipeline/pkg/pipeline/model"
)

const maxUint64Float = float64(^uint64(0))

func randomJitterSeed() (float64, error) {
	var buf [8]byte

	_, err := rand.Read(buf[:])
	if err != nil {
		return 0, errors.Wrap(err, "read jitter seed")
	}

	value := binary.LittleEndian.Uint64(buf[:])

	return float64(value) / maxUint64Float, nil
}

type stepRetryOption interface {
	OnStepRetry(parentStep, step *model.StepInfo, attempt int, computationDuration time.Duration) error
}

type retryOutcome[T any] struct {
	value T
}

func shouldRetry(ctx context.Context, policy *model.RetryPolicy, err error) bool {
	if err == nil {
		return false
	}

	if ctx.Err() != nil {
		return false
	}

	if policy == nil {
		return false
	}

	if policy.RetryOn != nil {
		return policy.RetryOn(err)
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	return true
}

func retryDelay(policy *model.RetryPolicy, attempt int) time.Duration {
	if policy == nil || policy.Backoff <= 0 {
		return 0
	}

	retryAttempt := attempt
	if retryAttempt < 1 {
		retryAttempt = 1
	}

	multiplier := 1 << (retryAttempt - 1)
	delay := time.Duration(multiplier) * policy.Backoff

	if policy.MaxBackoff > 0 && delay > policy.MaxBackoff {
		delay = policy.MaxBackoff
	}

	if policy.Jitter > 0 && delay > 0 {
		jitterSeed, err := randomJitterSeed()
		if err == nil {
			jitterFactor := (jitterSeed*2 - 1) * policy.Jitter

			delay = time.Duration(float64(delay) * (1 + jitterFactor))

			if delay < 0 {
				delay = 0
			}
		}
	}

	return delay
}

func sleepRetry(ctx context.Context, policy *model.RetryPolicy, attempt int) error {
	delay := retryDelay(policy, attempt)
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "context done")
	case <-timer.C:
		return nil
	}
}

func reportStepRetry(
	opts []model.PipelineOption,
	parentStep, step *model.StepInfo,
	attempt int,
	computationDuration time.Duration,
) error {
	for _, opt := range opts {
		retryOpt, ok := opt.(stepRetryOption)
		if !ok {
			continue
		}

		err := retryOpt.OnStepRetry(parentStep, step, attempt, computationDuration)
		if err != nil {
			return errors.Wrap(err, "unable to report retry")
		}
	}

	return nil
}

func executeWithRetry[T any](
	ctx context.Context,
	policy *model.RetryPolicy,
	attemptFn func() (T, error),
	reportRetry func(attempt int, duration time.Duration) error,
) (retryOutcome[T], time.Duration, error) {
	var zero T
	zeroOutcome := retryOutcome[T]{value: zero}

	if policy == nil || policy.MaxAttempts < 2 {
		start := time.Now()
		out, err := attemptFn()

		return retryOutcome[T]{value: out}, time.Since(start), err
	}

	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return zeroOutcome, 0, errors.Wrap(ctx.Err(), "context done")
		}

		start := time.Now()
		out, err := attemptFn()

		duration := time.Since(start)
		if err == nil {
			return retryOutcome[T]{value: out}, duration, nil
		}

		if !shouldRetry(ctx, policy, err) || attempt == policy.MaxAttempts {
			return zeroOutcome, duration, err
		}

		if reportRetry != nil {
			reportErr := reportRetry(attempt, duration)
			if reportErr != nil {
				return zeroOutcome, duration, reportErr
			}
		}

		sleepErr := sleepRetry(ctx, policy, attempt)
		if sleepErr != nil {
			return zeroOutcome, duration, sleepErr
		}
	}

	return zeroOutcome, 0, nil
}
