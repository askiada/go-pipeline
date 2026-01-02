package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

type rateLimiter struct {
	mu     sync.Mutex
	every  time.Duration
	burst  int
	last   time.Time
	tokens int
}

func newRateLimiter(policy *model.RateLimitPolicy) *rateLimiter {
	if policy == nil || policy.Every <= 0 {
		return nil
	}

	burst := policy.Burst
	burst = max(burst, 1)

	return &rateLimiter{
		every:  policy.Every,
		burst:  burst,
		tokens: burst,
	}
}

func (rl *rateLimiter) wait(ctx context.Context) error {
	if rl == nil {
		return nil
	}

	for {
		wait := rl.takeToken()
		if wait <= 0 {
			return nil
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()

			return errors.Wrap(ctx.Err(), "rate limit canceled")
		case <-timer.C:
		}
	}
}

func (rl *rateLimiter) takeToken() time.Duration {
	now := time.Now()

	rl.mu.Lock()
	defer rl.mu.Unlock()

	if rl.last.IsZero() {
		rl.last = now
	}

	if rl.every > 0 {
		elapsed := now.Sub(rl.last)

		add := int(elapsed / rl.every)
		if add > 0 {
			rl.tokens += add
			if rl.tokens > rl.burst {
				rl.tokens = rl.burst
			}

			rl.last = rl.last.Add(time.Duration(add) * rl.every)
		}
	}

	if rl.tokens > 0 {
		rl.tokens--

		return 0
	}

	if rl.every <= 0 {
		return 0
	}

	wait := rl.every - now.Sub(rl.last)
	wait = max(wait, 0)

	return wait
}
