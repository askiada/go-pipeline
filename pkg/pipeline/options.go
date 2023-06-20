package pipeline

type PipelineOption func(p *Pipeline)

func PipelineDrawer(svgFileName string) PipelineOption {
	return func(p *Pipeline) {
		p.drawer = newDrawer(svgFileName)
	}
}

func PipelineMeasure() PipelineOption {
	return func(p *Pipeline) {
		p.measure = newMeasure()
	}
}

type StepOption[O any] func(s *Step[O])

func StepConcurrency[O any](concurrent int) StepOption[O] {
	return func(s *Step[O]) {
		s.concurrent = concurrent
	}
}

type SplitterOption[I any] func(s *Splitter[I])

func SplitterBufferSize[I any](bufferSize int) SplitterOption[I] {
	return func(s *Splitter[I]) {
		s.bufferSize = bufferSize
	}
}
