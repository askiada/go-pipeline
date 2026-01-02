package drawer

import (
	"time"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/measure"
)

// Drawer describes how to render a pipeline graph.
type Drawer interface {
	// AddStep adds a step to the pipeline drawer.
	AddStep(stepname string) error
	// AddLink adds a link between parent and child steps.
	AddLink(panrentStepName, childrenStepName string) error
	// Draw creates the output graph.
	Draw() error
	// SetTotalTime sets the total time for the step.
	SetTotalTime(stepName string, totalTime time.Time) error
	// AddMeasure adds a Measure to the output.
	AddMeasure(measure measure.Measure) error
}
