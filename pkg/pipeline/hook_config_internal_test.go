package pipeline

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

type noopMetricsOption struct{}

func (noopMetricsOption) OnStepOutputMetrics(_, _ *model.StepInfo, _, _ time.Duration) error {
	return nil
}

func (noopMetricsOption) OnSplitterOutputMetrics(_, _ *model.StepInfo, _, _ time.Duration) error {
	return nil
}

func (noopMetricsOption) OnMergerOutputMetrics(_, _ *model.StepInfo, _ time.Duration) error {
	return nil
}

func (noopMetricsOption) OnSinkOutputMetrics(_, _ *model.StepInfo, _, _ time.Duration) error {
	return nil
}

func (noopMetricsOption) AfterSinkMetrics(_ *model.StepInfo, _ time.Duration) error {
	return nil
}

func TestHookConfigNilPipeline(t *testing.T) {
	t.Parallel()

	var pipe *Pipeline
	cfg := pipe.hookConfig()

	assert.Nil(t, cfg.opts)
	assert.Nil(t, cfg.metricsOpts)
	assert.False(t, cfg.outputMetrics)
	assert.False(t, cfg.timing)
	assert.False(t, cfg.drop)
	assert.False(t, cfg.errorRoute)
	assert.False(t, cfg.retry)
}

func TestHookConfigCopiesFlags(t *testing.T) {
	t.Parallel()

	pipe := &Pipeline{
		opts:                 []model.PipelineOption{PipelineDefaults{}},
		metricsOpts:          []model.PipelineMetricsOption{noopMetricsOption{}},
		outputMetricsEnabled: true,
		timingEnabled:        true,
		dropEnabled:          true,
		errorRouteEnabled:    true,
		retryEnabled:         true,
	}

	cfg := pipe.hookConfig()
	assert.Len(t, cfg.opts, 1)
	assert.Len(t, cfg.metricsOpts, 1)
	assert.True(t, cfg.outputMetrics)
	assert.True(t, cfg.timing)
	assert.True(t, cfg.drop)
	assert.True(t, cfg.errorRoute)
	assert.True(t, cfg.retry)
}
