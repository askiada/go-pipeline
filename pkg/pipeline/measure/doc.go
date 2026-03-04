// Package measure collects timing and drop metrics for pipeline runs.
//
// Use PipelineMeasure to attach a Measure to a pipeline.
//
// Example:
//
//	msr := measure.NewDefaultMeasure()
//	pipe, _ := pipeline.New(measure.PipelineMeasure(msr))
//	_ = pipe.Run(context.Background())
package measure
