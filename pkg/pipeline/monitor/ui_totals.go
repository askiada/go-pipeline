package monitor

import (
	"encoding/json"
	"maps"
)

type uiOutputTotals struct {
	Count       int64 `json:"count"`
	DurationMs  int64 `json:"duration_ms"`
	TransportMs int64 `json:"transport_ms"`
}

type uiTotals struct {
	outputs      map[string]uiOutputTotals
	drops        map[string]int64
	retries      map[string]int64
	errorRoutes  map[string]int64
	runTotalMs   int64
	runTotalSeen bool
}

type uiTotalsSnapshot struct {
	outputs      map[string]uiOutputTotals
	drops        map[string]int64
	retries      map[string]int64
	errorRoutes  map[string]int64
	runTotalMs   int64
	runTotalSeen bool
}

func (totals *uiTotals) apply(measurement string, tags map[string]string, fields map[string]any) {
	switch measurement {
	case "step_output", "splitter_output", "merger_output", "sink_output":
		totals.applyOutput(tags, fields)
	case "step_drop":
		totals.drops = totals.applyCount(tags, fields, totals.drops)
	case "step_retry":
		totals.retries = totals.applyCount(tags, fields, totals.retries)
	case "step_error_route":
		totals.errorRoutes = totals.applyCount(tags, fields, totals.errorRoutes)
	case "run_total":
		totals.applyRunTotal(fields)
	default:
	}
}

func (totals *uiTotals) applyOutput(tags map[string]string, fields map[string]any) {
	stepName := tagStepName(tags)
	if stepName == "" {
		return
	}

	if totals.outputs == nil {
		totals.outputs = make(map[string]uiOutputTotals)
	}

	current := totals.outputs[stepName]
	current.Count += fieldInt64(fields, "count")
	current.DurationMs += fieldInt64(fields, "duration_ms")
	current.TransportMs += fieldInt64(fields, "transport_ms")
	totals.outputs[stepName] = current
}

func (totals *uiTotals) applyCount(tags map[string]string, fields map[string]any, target map[string]int64) map[string]int64 {
	stepName := tagStepName(tags)
	if stepName == "" {
		return target
	}

	if target == nil {
		target = make(map[string]int64)
	}

	target[stepName] += fieldInt64(fields, "count")

	return target
}

func (totals *uiTotals) applyRunTotal(fields map[string]any) {
	totals.runTotalMs = fieldInt64(fields, "duration_ms")
	totals.runTotalSeen = true
}

func tagStepName(tags map[string]string) string {
	if tags == nil {
		return ""
	}

	return tags["step_name"]
}

func (totals *uiTotals) snapshot() uiTotalsSnapshot {
	if totals == nil {
		return uiTotalsSnapshot{}
	}

	snapshot := uiTotalsSnapshot{
		runTotalMs:   totals.runTotalMs,
		runTotalSeen: totals.runTotalSeen,
	}

	if len(totals.outputs) > 0 {
		snapshot.outputs = make(map[string]uiOutputTotals, len(totals.outputs))
		maps.Copy(snapshot.outputs, totals.outputs)
	}

	if len(totals.drops) > 0 {
		snapshot.drops = make(map[string]int64, len(totals.drops))
		maps.Copy(snapshot.drops, totals.drops)
	}

	if len(totals.retries) > 0 {
		snapshot.retries = make(map[string]int64, len(totals.retries))
		maps.Copy(snapshot.retries, totals.retries)
	}

	if len(totals.errorRoutes) > 0 {
		snapshot.errorRoutes = make(map[string]int64, len(totals.errorRoutes))
		maps.Copy(snapshot.errorRoutes, totals.errorRoutes)
	}

	return snapshot
}

func fieldInt64(fields map[string]any, key string) int64 {
	if len(fields) == 0 {
		return 0
	}

	value, ok := fields[key]
	if !ok || value == nil {
		return 0
	}

	switch typed := value.(type) {
	case int64:
		return typed
	case int:
		return int64(typed)
	case float32:
		return int64(typed)
	case float64:
		return int64(typed)
	case json.Number:
		num, err := typed.Int64()
		if err != nil {
			return 0
		}

		return num
	default:
		return 0
	}
}
