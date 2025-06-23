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
	startTime time.Time
}

// New creates a new pipeline.
func New(ctx context.Context, opts ...model.PipelineOption) (*Pipeline, error) {
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

// Run starts the pipeline and waits for it to finish.
func (p *Pipeline) Run() error {
	defer p.cancel()

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
