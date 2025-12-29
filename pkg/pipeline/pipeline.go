package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/askiada/go-pipeline/pkg/pipeline/model"
)

// Pipeline is a pipeline of steps.
type Pipeline struct {
	errcList  *errorChans
	cancel    context.CancelFunc
	opts      []model.PipelineOption
	defaults  PipelineDefaults
	buildErr  error
	startTime time.Time
	runners   []func(ctx context.Context)
	runnersMu sync.Mutex
}

// New creates a new pipeline.
func New(opts ...model.PipelineOption) (*Pipeline, error) {
	pipe := &Pipeline{
		errcList: &errorChans{},
	}

	pipelineOpts := make([]model.PipelineOption, 0, len(opts))
	for _, opt := range opts {
		if opt == nil {
			continue
		}

		isDefaults := false
		switch defaults := opt.(type) {
		case PipelineDefaults:
			pipe.defaults = defaults
			isDefaults = true
		case *PipelineDefaults:
			if defaults == nil {
				isDefaults = true

				continue
			}
			pipe.defaults = *defaults
			isDefaults = true
		}

		err := opt.New()
		if err != nil {
			return nil, errors.Wrap(err, "unable to apply pipeline option")
		}

		if !isDefaults {
			pipelineOpts = append(pipelineOpts, opt)
		}
	}

	pipe.opts = pipelineOpts

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
	if p == nil {
		return ErrPipelineMustBeSet
	}

	if p.buildErr != nil {
		return p.buildErr
	}

	if ctx == nil {
		return ErrContextMustBeSet
	}

	runCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.startTime = time.Now()

	runners := p.runnersSnapshot()
	for _, runner := range runners {
		if runner != nil {
			runner(runCtx)
		}
	}

	defer cancel()

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

func (p *Pipeline) addRunner(runner func(ctx context.Context)) {
	if p == nil || runner == nil {
		return
	}

	p.runnersMu.Lock()
	p.runners = append(p.runners, runner)
	p.runnersMu.Unlock()
}

func (p *Pipeline) runnersSnapshot() []func(ctx context.Context) {
	if p == nil {
		return nil
	}

	p.runnersMu.Lock()
	defer p.runnersMu.Unlock()

	runners := make([]func(ctx context.Context), len(p.runners))
	copy(runners, p.runners)

	return runners
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
