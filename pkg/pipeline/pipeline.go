package pipeline

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

type stepType string

const (
	rootStepType     = "root"
	normalStepType   = "step"
	splitterStepType = "splitter"
	sinkStepType     = "sink"
	mergeStepType    = "merger"
)

var (
	startStep = &Step[any]{details: &StepInfo{Name: "start"}}
	endStep   = &Step[any]{details: &StepInfo{Name: "end"}}
)

type Pipeline struct {
	ctx       context.Context
	errcList  *errorChans
	cancel    context.CancelFunc
	opts      []PipelineOption
	startTime time.Time
}

func New(ctx context.Context, opts ...PipelineOption) (*Pipeline, error) {
	dCtx, cancel := context.WithCancel(ctx)
	pipe := &Pipeline{
		ctx:       dCtx,
		errcList:  &errorChans{},
		cancel:    cancel,
		startTime: time.Now(),
		opts:      opts,
	}

	for _, opt := range opts {
		err := opt.New()
		if err != nil {
			return nil, errors.Wrap(err, "unable to apply pipeline option")
		}
	}

	return pipe, nil
}

// waitForPipeline waits for results from all error channels.
// It returns early on the first error.
func waitForPipeline(errs ...*errorChan) error {
	errc := mergeErrors(errs...)
	for err := range errc {
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Pipeline) finishRun() error {
	for _, opt := range p.opts {
		err := opt.Finish()
		if err != nil {
			return errors.Wrap(err, "unable to finish pipeline option")
		}
	}
	return nil
}

func (p *Pipeline) Run() error {
	defer p.cancel()
	err := waitForPipeline(p.errcList.list...)
	if err != nil {
		return err
	}
	return p.finishRun()
}
