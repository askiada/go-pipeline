package drawer

import (
	"time"

	"github.com/askiada/go-pipeline/pkg/pipeline/measure"
)

type Drawer interface {
	AddStep(stepname string) error
	AddLink(panrentStepName, childrenStepName string) error
	Draw() error
	SetTotalTime(stepName string, startTime time.Time) error
	AddMeasure(measure measure.Measure) error
}
