package pipeline

import (
	"sync"
	"time"
)

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
		mt := p.measure.addStep(outputStep.Name, 1)
		outputStep.metric = mt
	}

	wgrp := sync.WaitGroup{}
	wgrp.Add(len(steps))
	go func() {
		wgrp.Wait()
		close(errC)
		close(output)
	}()

	for _, step := range steps {
		go func(step *Step[I]) {
			defer wgrp.Done()
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
