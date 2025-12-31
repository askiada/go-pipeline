package pipeline

import "github.com/askiada/go-pipeline/pkg/pipeline/model"

// RunOption configures execution-time behaviour for a pipeline run.
type RunOption func(*model.RunOptions)

// RunDry enables dry-run mode, skipping runner execution but keeping validation and Finish hooks.
func RunDry() RunOption {
	return func(opts *model.RunOptions) {
		opts.DryRun = true
	}
}

func applyRunOptions(opts []RunOption) model.RunOptions {
	var runOpts model.RunOptions

	for _, opt := range opts {
		if opt != nil {
			opt(&runOpts)
		}
	}

	return runOpts
}
