package model

// RunOptions describes execution-time settings for a pipeline run.
type RunOptions struct {
	DryRun bool
}

// RunOptionAware is implemented by pipeline options that need run settings.
type RunOptionAware interface {
	SetRunOptions(opts RunOptions)
}
