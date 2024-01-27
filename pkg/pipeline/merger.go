package pipeline

import (
	"sync"
	"time"
)

func prepareMerger[I any](p *Pipeline, name string, steps ...*Step[I]) (*Step[I], error) {
	output := make(chan I)
	outputStep := &Step[I]{
		Type:   sinkStepType,
		Name:   name,
		Output: output,
	}
	err := p.feature.addStep(outputStep.Name)
	if err != nil {
		return nil, err
	}

	for _, step := range steps {
		err := p.feature.addLink(step.Name, outputStep.Name)
		if err != nil {
			return nil, err
		}
	}

	if p.feature.measure != nil {
		mt := p.feature.measure.addStep(outputStep.Name, 1)
		outputStep.metric = mt
	}
	return outputStep, nil
}

func AddMerger[I any](p *Pipeline, name string, bufferSize int, steps ...*Step[I]) (*Step[I], error) {
	errC := make(chan error, len(steps))
	decoratedError := newErrorChan(name, errC)
	outputStep, err := prepareMerger(p, name, steps...)
	if err != nil {
		return nil, err
	}
	wg := sync.WaitGroup{}
	wg.Add(len(steps))
	mergerBuffer := make([]chan I, len(steps))
	for i := range mergerBuffer {
		mergerBuffer[i] = make(chan I, bufferSize)
	}
	wgMerger := &sync.WaitGroup{}
	wgMerger.Add(len(mergerBuffer))
	go func() {
		wg.Wait()
		for _, buf := range mergerBuffer {
			close(buf)
		}
		wgMerger.Wait()
		close(errC)
		close(outputStep.Output)
	}()

	for i, step := range steps {
		localI := i
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
						errC <- p.ctx.Err()

						break outer
					case mergerBuffer[localI] <- entry:
						if outputStep.metric != nil {
							outputStep.metric.addChannel(step.Name, time.Since(startChan))
						}
					}
				}
			}
		}(step)
	}

	for _, buf := range mergerBuffer {
		localBuf := buf
		go func() {
			defer wgMerger.Done()
		outer:
			for {
				select {
				case <-p.ctx.Done():
					errC <- p.ctx.Err()
				case elem, ok := <-localBuf:
					if !ok {
						break outer
					}
					select {
					case outputStep.Output <- elem:
					case <-p.ctx.Done():
						errC <- p.ctx.Err()
					}
				}
			}
		}()
	}

	p.errcList.add(decoratedError)
	return outputStep, nil
}
