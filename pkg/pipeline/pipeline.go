package pipeline

import (
	"context"
	"sort"
	"time"

	"github.com/askiada/go-pipeline/internal/store"
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
	feature   *feature[string, string]
	startTime time.Time
}

func New(ctx context.Context, opts ...PipelineOption) (*Pipeline, error) {
	dCtx, cancel := context.WithCancel(ctx)
	p := &Pipeline{
		ctx:       dCtx,
		errcList:  &errorChans{},
		cancel:    cancel,
		startTime: time.Now(),
		feature:   &feature[string, string]{},
	}

	for _, opt := range opts {
		opt(p)
	}

	if p.feature.drawer != nil {
		p.feature.store = store.NewMemoryStore[string, string]()
		p.feature.graph = graph.NewWithStore(graph.StringHash, graph.Store[string, string](p.feature.store), graph.Directed())
	}

	if p.feature.drawer != nil {
		err := p.feature.addStep("start")
		if err != nil {
			return nil, err
		}
		err = p.feature.addStep("end")
		if err != nil {
			return nil, err
		}
	}

	if p.feature.measure != nil {
		mt := p.feature.measure.addStep("start", 1)
		p.feature.measure.start = mt
		mt = p.feature.measure.addStep("end", 1)
		p.feature.measure.end = mt
	}
	return p, nil
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
	if p.feature.measure == nil {
		return nil
	}
	p.feature.store.UpdateVertex("end", func(vp *graph.VertexProperties) {
		vp.Attributes["xlabel"] = time.Since(p.startTime).String()
	})

	allChanElapsed := make(map[time.Duration]string)
	sortedAllChanElapsed := []time.Duration{}
	for _, step := range p.feature.measure.steps {
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

	p.feature.maxAvgStep = time.Duration(0)
	p.feature.maxAvgEdge = time.Duration(0)
	for _, step := range p.feature.measure.steps {
		stepAvg := step.avgStep()
		if stepAvg > p.feature.maxAvgStep {
			p.feature.maxAvgStep = stepAvg
		}
		for _, info := range step.channelElapsed {
			if info.elapsed > p.feature.maxAvgEdge {
				p.feature.maxAvgEdge = info.elapsed
			}
		}
	}

	for name, step := range p.feature.measure.steps {
		stepAvg := step.avgStep()
		if stepAvg != 0 {
			p.feature.store.UpdateVertex(name, func(vp *graph.VertexProperties) {
				vp.Attributes["xlabel"] = stepAvg.String()
				vp.Weight = int64(p.feature.maxAvgStep - stepAvg)
			})
		}
		if step.endDuration > 0 {
			p.feature.store.UpdateVertex(name, func(vp *graph.VertexProperties) {
				vp.Attributes["xlabel"] += ", end: " + step.endDuration.String()
			})
		}

		for inputStep, info := range step.channelElapsed {
			if info.elapsed == 0 {
				continue
			}
			err := p.feature.graph.UpdateEdge(inputStep, name,
				graph.EdgeAttribute("label", info.elapsed.String()),
				graph.EdgeAttribute("fontcolor", "blue"),
				graph.EdgeAttribute("color", allChanElapsed[info.elapsed]), //nolint
				// Useful to compute slowest path
				graph.EdgeWeight(int64(p.feature.maxAvgEdge-info.elapsed)),
			)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *Pipeline) finishRun() error {
	if p.feature.drawer == nil {
		return nil
	}

	err := p.drawMeasure()
	if err != nil {
		return err
	}
	err = p.feature.drawer.draw(p.feature.graph)
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
