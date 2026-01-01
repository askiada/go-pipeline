package model

// StepDropKind describes why an item was dropped.
type StepDropKind string

// StepDropBufferFull, StepDropSendTimeout, and StepDropError describe drop reasons.
const (
	StepDropBufferFull  StepDropKind = "buffer_full"
	StepDropSendTimeout StepDropKind = "send_timeout"
	StepDropError       StepDropKind = "error"
)

// StepError captures an item that failed in a step.
type StepError struct {
	StepName string
	Item     any
	Err      error
}

// StepDropObserver observes dropped items.
type StepDropObserver interface {
	OnStepDrop(step *StepInfo, kind StepDropKind) error
}

// StepErrorRouteObserver observes routed step errors.
type StepErrorRouteObserver interface {
	OnStepErrorRoute(step *StepInfo) error
}
