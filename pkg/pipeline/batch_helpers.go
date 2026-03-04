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

type batchTracker struct {
	maxSize    int
	maxWait    time.Duration
	batchStart time.Time
	count      int
	waitTotal  time.Duration
	timer      *time.Timer
	timerC     <-chan time.Time
}

func newBatchTracker(maxSize int, maxWait time.Duration) *batchTracker {
	return &batchTracker{
		maxSize: maxSize,
		maxWait: maxWait,
	}
}

func (tracker *batchTracker) resetTimer() {
	tracker.timer, tracker.timerC = resetBatchTimer(tracker.timer, tracker.maxWait)
}

func (tracker *batchTracker) clearTimer() {
	stopBatchTimer(tracker.timer)
	tracker.timer = nil
	tracker.timerC = nil
}

func (tracker *batchTracker) startBatch() {
	if !tracker.batchStart.IsZero() {
		return
	}

	tracker.batchStart = time.Now()
	tracker.resetTimer()
}

func (tracker *batchTracker) shouldFlushForTime() bool {
	if tracker.maxWait <= 0 || tracker.batchStart.IsZero() {
		return false
	}

	return time.Since(tracker.batchStart) >= tracker.maxWait
}

func (tracker *batchTracker) recordWait(wait time.Duration) {
	tracker.waitTotal += wait
}

func (tracker *batchTracker) recordItem() {
	tracker.count++
}

func (tracker *batchTracker) shouldFlushForSize() bool {
	return tracker.count >= tracker.maxSize
}

func (tracker *batchTracker) snapshotAndReset() (int, time.Duration) {
	count := tracker.count
	waitTotal := tracker.waitTotal

	tracker.reset()

	return count, waitTotal
}

func (tracker *batchTracker) reset() {
	tracker.count = 0
	tracker.waitTotal = 0
	tracker.batchStart = time.Time{}
	tracker.clearTimer()
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
