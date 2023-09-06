package pipeline

import (
	"context"
	"sort"
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
	pipe := &Pipeline{
		ctx:       dCtx,
		errcList:  &errorChans{},
		cancel:    cancel,
		startTime: time.Now(),
	}

	for _, opt := range opts {
		opt(pipe)
	}

	if pipe.drawer != nil {
		err := pipe.drawer.addStep("start")
		if err != nil {
			return nil, err
		}
		err = pipe.drawer.addStep("end")
		if err != nil {
			return nil, err
		}
	}

	if pipe.measure != nil {
		mt := pipe.measure.addStep("start", 1)
		pipe.measure.start = mt
		mt = pipe.measure.addStep("end", 1)
		pipe.measure.end = mt
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

func (p *Pipeline) drawMeasure() error {
	if p.measure == nil {
		return nil
	}

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
		fraction := time.Duration(1)
		if maxValue > minValue {
			fraction = (curr - minValue) / (maxValue - minValue)
		}
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

		if step.endDuration > 0 {
			properties.Attributes["xlabel"] += ", end: " + step.endDuration.String()
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
	return nil
}

func (p *Pipeline) finishRun() error {
	if p.drawer == nil {
		return nil
	}

	err := p.drawMeasure()
	if err != nil {
		return err
	}
	err = p.drawer.draw()
	if err != nil {
		return err
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
