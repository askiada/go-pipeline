// Package monitor sends pipeline run data to Telegraf and an optional local UI.
//
// Example:
//
//	cfg := &monitor.Config{EnableUI: true}
//	pipe, _ := pipeline.New(monitor.PipelineMonitor(cfg))
//	_ = pipe.Run(context.Background())
package monitor
