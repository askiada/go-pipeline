package model

type stepType string

const (
	RootStepType     = "root"
	NormalStepType   = "step"
	SplitterStepType = "splitter"
	SinkStepType     = "sink"
	MergeStepType    = "merger"
)

type StepInfo struct {
	Type       stepType
	Name       string
	Concurrent int
	BufferSize int
}

var (
	StartStep = &Step[any]{Details: &StepInfo{Name: "start"}}
	EndStep   = &Step[any]{Details: &StepInfo{Name: "end"}}
)

type Step[O any] struct {
	Output   chan O
	KeepOpen bool
	Details  *StepInfo
}
