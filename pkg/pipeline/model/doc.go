// Package model holds shared types used by pipeline options and hooks.
// It is not a full pipeline API by itself.
//
// Example:
//
//	info := &model.StepInfo{
//		Name:       "parse",
//		Type:       model.NormalStepType,
//		Concurrent: 1,
//	}
//
//	_ = info
package model
