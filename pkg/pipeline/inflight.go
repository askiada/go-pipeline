package pipeline

import (
	"context"
	"fmt"
)

type inFlightLimiter struct {
	sem chan struct{}
}

func newInFlightLimiter(maxInFlight int) *inFlightLimiter {
	if maxInFlight < 1 {
		return nil
	}

	return &inFlightLimiter{sem: make(chan struct{}, maxInFlight)}
}

func (l *inFlightLimiter) acquire(ctx context.Context) error {
	if l == nil {
		return nil
	}

	select {
	case l.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("max in-flight canceled: %w", ctx.Err())
	}
}

func (l *inFlightLimiter) release() {
	if l == nil {
		return
	}

	select {
	case <-l.sem:
	default:
	}
}
