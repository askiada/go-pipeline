package model

import "time"

type stepType string

const (
	// RootStepType  entry point of the pipeline.
	RootStepType stepType = "root"
	// NormalStepType a regular step in the pipeline.
	NormalStepType stepType = "step"
	// SplitterStepType a step that splits the input into multiple outputs.
	SplitterStepType stepType = "splitter"
	// MergerStepType a step that merges multiple inputs into a single output.
	MergerStepType stepType = "merger"
	// SinkStepType a step that consumes the output of the pipeline.
	SinkStepType stepType = "sink"
)

// StepInfo describes a step for hooks, metrics, and reporting.
type StepInfo struct {
	// Type is the step kind.
	Type stepType
	// Name is the step name and should be unique in the pipeline.
	Name string
	// Concurrent is how many goroutines run this step. Default is 1.
	Concurrent int
	// BufferSize is the output channel buffer size. Default is 0.
	BufferSize int
}

var (
	// StartStep is a sentinel step used as the parent of root steps.
	//
	//nolint:gochecknoglobals // This is a global constant that represents the start of the pipeline.
	StartStep = &Step[any]{Details: &StepInfo{Name: "start", Concurrent: 1}}
	// EndStep is a sentinel step used as the child of sink steps.
	//
	//nolint:gochecknoglobals // This is a global constant that represents the start of the pipeline.
	EndStep = &Step[any]{Details: &StepInfo{Name: "end", Concurrent: 1}}
)

// Step holds runtime settings and channels for a pipeline step.
// Most fields are set by step options before the pipeline runs.
type Step[O any] struct {
	Output   chan O
	KeepOpen bool
	// ErrorOutput routes per-item errors when enabled.
	ErrorOutput chan StepError
	// ErrorStep exposes error routing as a normal step in the pipeline.
	ErrorStep *Step[StepError]
	// ErrorOutputEnabled controls whether the error channel is created.
	ErrorOutputEnabled bool
	// ErrorOutputBufferSize configures the error channel buffer size.
	ErrorOutputBufferSize int
	// RetryPolicy configures per-item retries for step functions.
	RetryPolicy *RetryPolicy
	// RateLimitPolicy configures per-item rate limiting for step functions.
	RateLimitPolicy *RateLimitPolicy
	// Timeout limits how long a single step invocation can run.
	Timeout time.Duration
	// MaxInFlight caps the number of in-flight items per step.
	MaxInFlight int
	// DropOnOutputFull drops items when output buffers are full.
	DropOnOutputFull bool
	// DropOnOutputTimeout drops items if output sends block longer than this duration.
	DropOnOutputTimeout time.Duration
	// DropOnError drops items after retries are exhausted instead of propagating the error.
	DropOnError bool
	// BatchPolicy configures batching/windowing for batch steps.
	BatchPolicy *BatchPolicy
	Details     *StepInfo
}

// ErrorChan returns the error routing channel for the step, if enabled.
func (s *Step[O]) ErrorChan() <-chan StepError {
	if s == nil {
		return nil
	}

	return s.ErrorOutput
}
