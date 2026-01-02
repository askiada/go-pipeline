package pipeline

import "github.com/askiada/go-pipeline/pkg/pipeline/model"

type hookConfig struct {
	opts          []model.PipelineOption
	metricsOpts   []model.PipelineMetricsOption
	outputMetrics bool
	timing        bool
	drop          bool
	errorRoute    bool
	retry         bool
}

func (p *Pipeline) hookConfig() hookConfig {
	if p == nil {
		return hookConfig{}
	}

	return hookConfig{
		opts:          p.opts,
		metricsOpts:   p.metricsOpts,
		outputMetrics: p.outputMetricsEnabled,
		timing:        p.timingEnabled,
		drop:          p.dropEnabled,
		errorRoute:    p.errorRouteEnabled,
		retry:         p.retryEnabled,
	}
}
