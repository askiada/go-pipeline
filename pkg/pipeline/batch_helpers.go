package pipeline

import (
	"fmt"
	"time"
)

//nolint:gocritic // Unnamed returns keep the helper signature compact at call sites.
func resetBatchTimer(timer *time.Timer, maxWait time.Duration) (*time.Timer, <-chan time.Time) {
	if maxWait <= 0 {
		return timer, nil
	}

	if timer == nil {
		timer = time.NewTimer(maxWait)

		return timer, timer.C
	}

	stopBatchTimer(timer)
	timer.Reset(maxWait)

	return timer, timer.C
}

func reportBatchOutput[I any, O any](
	cfg hookConfig,
	input *Step[I],
	output *Step[O],
	count int,
	waitTotal time.Duration,
	computeTotal time.Duration,
) error {
	for _, opt := range cfg.opts {
		err := opt.OnStepOutput(input.Details, output.Details)
		if err != nil {
			return fmt.Errorf("unable to run before step function: %w", err)
		}
	}

	if cfg.outputMetrics {
		avgWait := time.Duration(0)
		avgCompute := time.Duration(0)

		if count > 0 {
			avgWait = waitTotal / time.Duration(count)
			avgCompute = computeTotal / time.Duration(count)
		}

		for _, opt := range cfg.metricsOpts {
			err := opt.OnStepOutputMetrics(input.Details, output.Details, avgWait, avgCompute)
			if err != nil {
				return fmt.Errorf("unable to run before step function: %w", err)
			}
		}
	}

	return nil
}
