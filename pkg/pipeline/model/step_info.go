package model

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

// StepInfo contains information about a step in the pipeline.
type StepInfo struct {
	// Type represents the type of the step.
	Type stepType
	// Name is the name of the step.
	// It is used to identify the step in the pipeline.
	// It should be unique within the pipeline.
	Name string
	// Concurrent is the number of concurrent goroutines that can run this step.
	// It is used to control the concurrency of the step.
	// If it is set to 1, the step will run sequentially.
	Concurrent int
	// BufferSize is the size of the output channel buffer for this step.
	// If it is set to 0, the output channel will be unbuffered.
	BufferSize int
}

var (
	// StartStep is a special step that represents the start of the pipeline.
	// It is used to initialise the pipeline and is not meant to be used as a regular step.
	// It is the parent of all root steps in the pipeline.
	//
	//nolint:gochecknoglobals // This is a global constant that represents the start of the pipeline.
	StartStep = &Step[any]{Details: &StepInfo{Name: "start"}}
	// EndStep is a special step that represents the end of the pipeline.
	// It is used to signal the end of the pipeline and is not meant to be used as a regular step.
	// It is the child of all sink steps in the pipeline.
	//
	//nolint:gochecknoglobals // This is a global constant that represents the start of the pipeline.
	EndStep = &Step[any]{Details: &StepInfo{Name: "end"}}
)

// Step represents a step in the pipeline.
// It contains an output channel for the step's output, a flag to keep the channel open
// after the step is done, and additional details about the step.
// The output channel is used to send data from the step to the next step in the pipeline.
// The KeepOpen flag indicates whether the output channel should remain open after the step is done,
// allowing for further data to be sent to it.
type Step[O any] struct {
	Output   chan O
	KeepOpen bool
	Details  *StepInfo
}
