package model

// RunOptions describes execution-time settings for a pipeline run.
type RunOptions struct {
	// DryRun skips step execution but still runs validation and Finish hooks.
	DryRun bool
}

// RunOptionAware is implemented by pipeline options that need run settings.
type RunOptionAware interface {
	SetRunOptions(opts RunOptions)
}
