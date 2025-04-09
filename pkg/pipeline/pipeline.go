package pipeline

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/askiada/go-pipeline/pkg/pipeline/model"
)

// Pipeline is a pipeline of steps.
type Pipeline struct {
	errcList  *errorChans
	opts      []model.PipelineOption
	startTime time.Time
	goFn      []func(ctx context.Context)
}

// New creates a new pipeline.
func New(opts ...model.PipelineOption) (*Pipeline, error) {
	pipe := &Pipeline{
		errcList:  &errorChans{},
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

// Run starts the pipeline and waits for it to finish.
func (p *Pipeline) Run(ctx context.Context) error {
	dCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, fn := range p.goFn {
		go fn(dCtx)
	}

	// Wait for all steps to finish.
	err := waitForPipeline(p.errcList.list...)
	if err != nil {
		return err
	}

	return p.finishRun()
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
