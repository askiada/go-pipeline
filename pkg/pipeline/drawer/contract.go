package drawer

import (
	"time"

	"github.com/askiada/go-pipeline/pkg/pipeline/measure"
)

// Drawer is an interface that defines the methods for drawing a pipeline.
type Drawer interface {
	// AddStep adds a step to the pipeline drawer.
	AddStep(stepname string) error
	// AddLink adds a link between parent and children steps.
	AddLink(panrentStepName, childrenStepName string) error
	// Draw creates a file with the pipeline graph.
	Draw() error
	// SetTotalTime sets the total time for the step.
	SetTotalTime(stepName string, totalTime time.Time) error
	// AddMeasure adds a measure to the pipeline drawer.
	AddMeasure(measure measure.Measure) error
}
