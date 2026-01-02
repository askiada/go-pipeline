package pipeline

import "github.com/askiada/go-pipeline/v2/pkg/pipeline/model"

// RunOption changes execution-time behaviour for a pipeline run.
// If no RunOption is set, defaults apply.
type RunOption func(*model.RunOptions)

// RunDry enables dry-run mode.
// It skips step execution but still runs validation and Finish hooks.
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
