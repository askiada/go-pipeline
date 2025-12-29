package pipeline

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/askiada/go-pipeline/pkg/pipeline/model"
)

// Pipeline is a pipeline of steps.
type Pipeline struct {
	ctx       context.Context //nolint:containedctx // The context for the pipeline. It is used to cancel the pipeline.
	errcList  *errorChans
	cancel    context.CancelFunc
	opts      []model.PipelineOption
	defaults  PipelineDefaults
	buildErr  error
	startTime time.Time
}

// New creates a new pipeline.
func New(ctx context.Context, defaults PipelineDefaults, opts ...model.PipelineOption) (*Pipeline, error) {
	dCtx, cancel := context.WithCancel(ctx)
	pipe := &Pipeline{
		ctx:       dCtx,
		errcList:  &errorChans{},
		cancel:    cancel,
		startTime: time.Now(),
		opts:      opts,
		defaults:  defaults,
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
func (p *Pipeline) Run() error {
	if p == nil {
		return ErrPipelineMustBeSet
	}

	if p.buildErr != nil {
		p.cancel()
		return p.buildErr
	}

	defer p.cancel()

	err := waitForPipeline(p.errcList.list...)
	if err != nil {
		return err
	}

	return p.finishRun()
}

// Err returns the first construction error, if any.
// Run will return the same error before executing the pipeline.
func (p *Pipeline) Err() error {
	if p == nil {
		return ErrPipelineMustBeSet
	}

	return p.buildErr
}

func (p *Pipeline) recordErr(err error) {
	if p == nil || err == nil || p.buildErr != nil {
		return
	}

	p.buildErr = err
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
