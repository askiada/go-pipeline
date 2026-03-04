// Package drawer renders pipeline graphs with Graphviz.
//
// Use PipelineDrawer to attach a Drawer to a pipeline run.
//
// Example:
//
//	drw := drawer.NewSVGDrawer("graph.svg")
//	msr := measure.NewDefaultMeasure()
//	pipe, _ := pipeline.New(drawer.PipelineDrawer(drw, msr))
//	_ = pipe.Run(context.Background())
package drawer
