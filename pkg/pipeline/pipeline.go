package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

// Step is the pipeline step type used by the public API.
// Steps are created by functions like Root, OneToOne, and Sink.
type Step[O any] = model.Step[O]

// StepInfo holds step metadata used by options, metrics, and reporting.
type StepInfo = model.StepInfo

// Pipeline holds the built step graph and run settings.
// A pipeline can be run once; build errors are stored and returned on Run.
type Pipeline struct {
	errcList             *errorChans
	cancel               context.CancelFunc
	opts                 []model.PipelineOption
	metricsOpts          []model.PipelineMetricsOption
	defaults             PipelineDefaults
	buildErr             error
	startTime            time.Time
	ran                  bool
	runners              []func(ctx context.Context)
	runnersMu            sync.Mutex
	outputMetricsEnabled bool
	timingEnabled        bool
	dropEnabled          bool
	errorRouteEnabled    bool
	retryEnabled         bool
}

type optionCapabilities struct {
	outputMetricsEnabled bool
	dropEnabled          bool
	errorRouteEnabled    bool
	retryEnabled         bool
}

// New creates a new pipeline with optional pipeline options.
// Options can add metrics, monitoring, or defaults for steps and splitters.
func New(opts ...model.PipelineOption) (*Pipeline, error) {
	pipe := &Pipeline{
		errcList: &errorChans{},
	}

	pipelineOpts, metricsOpts, caps, err := collectPipelineOptions(pipe, opts)
	if err != nil {
		return nil, err
	}

	pipe.opts = pipelineOpts
	pipe.metricsOpts = metricsOpts
	pipe.outputMetricsEnabled = caps.outputMetricsEnabled
	pipe.retryEnabled = caps.retryEnabled
	pipe.dropEnabled = caps.dropEnabled
	pipe.errorRouteEnabled = caps.errorRouteEnabled
	pipe.timingEnabled = caps.outputMetricsEnabled || caps.retryEnabled

	return pipe, nil
}

func collectPipelineOptions(
	pipe *Pipeline,
	opts []model.PipelineOption,
) ([]model.PipelineOption, []model.PipelineMetricsOption, optionCapabilities, error) {
	pipelineOpts := make([]model.PipelineOption, 0, len(opts))
	metricsOpts := make([]model.PipelineMetricsOption, 0, len(opts))
	caps := optionCapabilities{}

	for _, opt := range opts {
		if opt == nil {
			continue
		}

		isDefaults, skip := applyPipelineDefaults(pipe, opt)
		if skip {
			continue
		}

		err := opt.New()
		if err != nil {
			return nil, nil, optionCapabilities{}, fmt.Errorf("unable to apply pipeline option: %w", err)
		}

		if metricsOpt, ok := opt.(model.PipelineMetricsOption); ok {
			metricsOpts = append(metricsOpts, metricsOpt)
			caps.outputMetricsEnabled = true
		}

		caps.dropEnabled = caps.dropEnabled || hasDropObserver(opt)
		caps.errorRouteEnabled = caps.errorRouteEnabled || hasErrorRouteObserver(opt)
		caps.retryEnabled = caps.retryEnabled || hasRetryOption(opt)

		if !isDefaults {
			pipelineOpts = append(pipelineOpts, opt)
		}
	}

	return pipelineOpts, metricsOpts, caps, nil
}

//nolint:gocritic // Two booleans are clearer without naming on this helper.
func applyPipelineDefaults(pipe *Pipeline, opt model.PipelineOption) (bool, bool) {
	switch defaults := opt.(type) {
	case PipelineDefaults:
		pipe.defaults = defaults

		return true, false
	case *PipelineDefaults:
		if defaults == nil {
			return false, true
		}

		pipe.defaults = *defaults

		return true, false
	default:
		return false, false
	}
}

func hasDropObserver(opt model.PipelineOption) bool {
	_, ok := opt.(model.StepDropObserver)

	return ok
}

func hasErrorRouteObserver(opt model.PipelineOption) bool {
	_, ok := opt.(model.StepErrorRouteObserver)

	return ok
}

func hasRetryOption(opt model.PipelineOption) bool {
	_, ok := opt.(stepRetryOption)

	return ok
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
// It returns the first error from any step and stops other work.
// Run options can change behaviour, for example RunDry skips execution.
func (p *Pipeline) Run(ctx context.Context, opts ...RunOption) error {
	if p == nil {
		return ErrPipelineMustBeSet
	}

	if p.buildErr != nil {
		return p.buildErr
	}

	if ctx == nil {
		return ErrContextMustBeSet
	}

	if p.ran {
		return ErrPipelineAlreadyRan
	}

	runOpts := applyRunOptions(opts)
	p.setRunOptions(runOpts)

	if runOpts.DryRun {
		return p.finishRun()
	}

	runCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	p.ran = true
	if p.outputMetricsEnabled {
		p.startTime = time.Now()
	}

	runners := p.runnersSnapshot()
	for _, runner := range runners {
		if runner != nil {
			runner(runCtx)
		}
	}

	defer cancel()

	err := waitForPipeline(p.errcList.list...)
	finishErr := p.finishRun()

	if err != nil {
		return err
	}

	return finishErr
}

// Err returns the first construction error, if any.
// Run will return the same error before executing the pipeline.
func (p *Pipeline) Err() error {
	if p == nil {
		return ErrPipelineMustBeSet
	}

	return p.buildErr
}

func (p *Pipeline) recordErr(name string, err error) {
	if p == nil || err == nil || p.buildErr != nil {
		return
	}

	p.buildErr = fmt.Errorf("pipeline build error in step '%s': %w", name, err)
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
			return fmt.Errorf("unable to finish pipeline option: %w", err)
		}
	}

	return nil
}

func (p *Pipeline) setRunOptions(runOpts model.RunOptions) {
	for _, opt := range p.opts {
		if awareOpt, ok := opt.(model.RunOptionAware); ok {
			awareOpt.SetRunOptions(runOpts)
		}
	}
}
