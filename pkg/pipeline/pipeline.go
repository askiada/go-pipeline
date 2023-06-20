package pipeline

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/dominikbraun/graph"
	"gopkg.in/go-playground/colors.v1"
)

type stepType string

const (
	rootStepType     = "root"
	normalStepType   = "step"
	splitterStepType = "splitter"
	sinkStepType     = "sink"
)

type Step[O any] struct {
	Type       stepType
	Name       string
	Output     chan O
	concurrent int
	metric     *metric
}

type Pipeline struct {
	ctx       context.Context
	errcList  *errorChans
	cancel    context.CancelFunc
	drawer    *drawer
	measure   *measure
	startTime time.Time
}

func New(ctx context.Context, opts ...PipelineOption) (*Pipeline, error) {
	dCtx, cancel := context.WithCancel(ctx)
	p := &Pipeline{
		ctx:       dCtx,
		errcList:  &errorChans{},
		cancel:    cancel,
		startTime: time.Now(),
	}

	for _, opt := range opts {
		opt(p)
	}

	if p.drawer != nil {
		err := p.drawer.addStep("start")
		if err != nil {
			return nil, err
		}
		err = p.drawer.addStep("end")
		if err != nil {
			return nil, err
		}
	}

	if p.measure != nil {
		mt := p.measure.addStep("start")
		p.measure.start = mt
		mt = p.measure.addStep("end")
		p.measure.end = mt
	}
	return p, nil
}

func prepareRootStep[O any](p *Pipeline, step *Step[O]) error {
	if p.drawer != nil {
		err := p.drawer.addStep(step.Name)
		if err != nil {
			return err
		}
		err = p.drawer.addLink("start", step.Name)
		if err != nil {
			return err
		}
	}
	if p.measure != nil {
		mt := p.measure.addStep(step.Name)
		step.metric = mt
	}
	return nil
}

func AddRootStep[O any](p *Pipeline, name string, stepFn func(ctx context.Context, rootChan chan<- O) error) (*Step[O], error) {
	if p == nil {
		return nil, ErrPipelineMustBeSet
	}

	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)
	output := make(chan O)
	step := &Step[O]{
		Type:   rootStepType,
		Name:   name,
		Output: output,
	}
	err := prepareRootStep(p, step)
	if err != nil {
		return nil, err
	}
	go func() {
		defer func() {
			close(output)
			close(errC)
		}()
		err := stepFn(p.ctx, output)
		if err != nil {
			errC <- err
		}
	}()
	p.errcList.add(decoratedError)

	return step, nil
}

func prepareStep[I, O any](p *Pipeline, input *Step[I], step *Step[O]) error {
	if p.drawer != nil {
		err := p.drawer.addStep(step.Name)
		if err != nil {
			return err
		}
		err = p.drawer.addLink(input.Name, step.Name)
		if err != nil {
			return err
		}
	}

	if p.measure != nil {
		mt := p.measure.addStep(step.Name)
		step.metric = mt
	}
	return nil
}

func addStep[I any, O any](p *Pipeline, name string, input *Step[I], stepToStepFn func(ctx context.Context, input *Step[I], output *Step[O]) error, opts ...StepOption[O]) (*Step[O], error) {
	if p == nil {
		return nil, ErrPipelineMustBeSet
	}
	if input == nil {
		return nil, ErrInputMustBeSet
	}

	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)
	output := make(chan O)
	step := &Step[O]{
		Type:   normalStepType,
		Name:   name,
		Output: output,
	}
	for _, opt := range opts {
		opt(step)
	}
	err := prepareStep(p, input, step)
	if err != nil {
		return nil, err
	}
	go func() {
		defer func() {
			close(errC)
			close(output)
		}()
		err := stepToStepFn(p.ctx, input, step)
		if err != nil {
			errC <- err
		}
	}()
	p.errcList.add(decoratedError)

	return step, nil
}

func AddStepOneToOne[I any, O any](p *Pipeline, name string, input *Step[I], oneToOneFn func(context.Context, I) (O, error), opts ...StepOption[O]) (*Step[O], error) {
	return addStep(p, name, input, func(ctx context.Context, in *Step[I], out *Step[O]) error {
		return oneToOne(ctx, in, out, oneToOneFn)
	}, opts...)
}

func AddStepOneToMany[I any, O any](p *Pipeline, name string, input *Step[I], oneToManyFn func(context.Context, I) ([]O, error), opts ...StepOption[O]) (*Step[O], error) {
	return addStep(p, name, input, func(ctx context.Context, in *Step[I], out *Step[O]) error {
		return oneToMany(ctx, in, out, oneToManyFn)
	})
}

func AddSink[I any](p *Pipeline, name string, input *Step[I], sinkFn func(ctx context.Context, input I) error) error {
	if p == nil {
		return ErrPipelineMustBeSet
	}
	if input == nil {
		return ErrInputMustBeSet
	}
	step := Step[I]{
		Type: sinkStepType,
		Name: name,
	}
	if p.drawer != nil {
		err := p.drawer.addStep(step.Name)
		if err != nil {
			return err
		}
		err = p.drawer.addLink(input.Name, step.Name)
		if err != nil {
			return err
		}
	}
	if p.measure != nil {
		mt := p.measure.addStep(step.Name)
		step.metric = mt

		err := p.drawer.addLink(step.Name, "end")
		if err != nil {
			return err
		}
	}

	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)
	go func() {
		defer func() {
			close(errC)
		}()
	outer:
		for {
			startInputChan := time.Now()
			select {
			case <-p.ctx.Done():
				errC <- p.ctx.Err()

				break outer
			case in, ok := <-input.Output:
				if !ok {
					break outer
				}
				endInputChan := time.Since(startInputChan)

				startFn := time.Now()
				err := sinkFn(p.ctx, in)
				if err != nil {
					errC <- err
				}
				endFn := time.Since(startFn)
				if step.metric != nil {
					step.metric.add(endFn)
					step.metric.addChannel(input.Name, endInputChan-endFn)
				}
			}
		}
	}()
	p.errcList.add(decoratedError)

	return nil
}

func AddMerger[I any](p *Pipeline, name string, steps ...*Step[I]) (*Step[I], error) {
	errC := make(chan error, len(steps))
	decoratedError := newErrorChan(name, errC)
	output := make(chan I)
	outputStep := Step[I]{
		Type:   sinkStepType,
		Name:   name,
		Output: output,
	}
	if p.drawer != nil {
		err := p.drawer.addStep(outputStep.Name)
		if err != nil {
			return nil, err
		}

		for _, step := range steps {
			err := p.drawer.addLink(step.Name, outputStep.Name)
			if err != nil {
				return nil, err
			}
		}
	}
	if p.measure != nil {
		mt := p.measure.addStep(outputStep.Name)
		outputStep.metric = mt
	}

	wg := sync.WaitGroup{}
	wg.Add(len(steps))
	go func() {
		wg.Wait()
		close(errC)
		close(output)
	}()

	for _, step := range steps {
		go func(step *Step[I]) {
			defer wg.Done()
		outer:
			for {
				startChan := time.Now()
				select {
				case <-p.ctx.Done():
					errC <- p.ctx.Err()
					break outer
				case entry, ok := <-step.Output:
					if !ok {
						break outer
					}
					select {
					case <-p.ctx.Done():
						return
					case output <- entry:
						if outputStep.metric != nil {
							outputStep.metric.addChannel(step.Name, time.Since(startChan))
						}
					}
				}
			}
		}(step)
	}

	p.errcList.add(decoratedError)
	return &outputStep, nil
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

func (p *Pipeline) Run() error {
	defer p.cancel()
	err := waitForPipeline(p.errcList.list...)
	if err != nil {
		return err
	}
	if p.drawer != nil {
		if p.measure != nil {
			_, properties, err := p.drawer.graph.VertexWithProperties("end")
			if err != nil {
				return err
			}
			properties.Attributes["xlabel"] = time.Since(p.startTime).String()

			allChanElapsed := make(map[time.Duration]string)
			sortedAllChanElapsed := []time.Duration{}
			for _, step := range p.measure.steps {
				channelElapsed := step.avgChannel()
				for _, info := range channelElapsed {
					if info.elapsed == 0 {
						continue
					}
					if _, ok := allChanElapsed[info.elapsed]; ok {
						continue
					}
					allChanElapsed[info.elapsed] = ""
					sortedAllChanElapsed = append(sortedAllChanElapsed, info.elapsed)
				}
			}
			sort.Slice(sortedAllChanElapsed, func(i, j int) bool {
				return sortedAllChanElapsed[i] > sortedAllChanElapsed[j]
			})
			redColor, err := colors.RGB(255, 0, 0)
			if err != nil {
				return err
			}
			maxValue := sortedAllChanElapsed[0]
			minValue := sortedAllChanElapsed[len(sortedAllChanElapsed)-1]

			allChanElapsed[maxValue] = redColor.ToHEX().String()
			for curr := range allChanElapsed {
				fraction := (curr - minValue) / (maxValue - minValue)
				red := 240 * fraction
				blue := -240*fraction + 240
				redColor, err := colors.RGB(uint8(red), 0, uint8(blue))
				if err != nil {
					return err
				}
				allChanElapsed[curr] = redColor.ToHEX().String()
			}

			for name, step := range p.measure.steps {
				_, properties, err := p.drawer.graph.VertexWithProperties(name)
				if err != nil {
					return err
				}
				stepAvg := step.avgStep()
				if stepAvg != 0 {
					properties.Attributes["xlabel"] = stepAvg.String()
				}

				for inputStep, info := range step.channelElapsed {
					if info.elapsed == 0 {
						continue
					}
					err := p.drawer.graph.UpdateEdge(inputStep, name,
						graph.EdgeAttribute("label", info.elapsed.String()),
						graph.EdgeAttribute("fontcolor", "blue"),
						graph.EdgeAttribute("color", allChanElapsed[info.elapsed]), //nolint
					)
					if err != nil {
						return err
					}
				}
			}
		}
		err := p.drawer.draw()
		if err != nil {
			return err
		}
	}
	/*if p.measure != nil {
		for name, step := range p.measure.steps {
			fmt.Printf("%s : %s\n", name, step.avgStep().String())
		}
	}*/
	return nil
}
