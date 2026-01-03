package monitor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline"
	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

const (
	defaultTelegrafNetwork = "udp"
	defaultTelegrafAddress = "127.0.0.1:8094"
	defaultBufferSize      = 1024
	baseTagCapacity        = 4
	defaultLineBuilderSize = 128
	defaultUIBindAddr      = "127.0.0.1:8096"
)

// Config configures live monitoring.
//
// Defaults:
// - TelegrafNet: "udp"
// - TelegrafAddr: "127.0.0.1:8094"
// - BufferSize: 1024
// - BindAddr: "127.0.0.1:8096" (only when EnableUI is true)
// - RunID: random
// - RunName: PipelineName or RunID
// - Origin: hostname:pid.
type Config struct {
	RunID         string
	RunName       string
	Origin        string
	PipelineName  string
	BindAddr      string
	EnableUI      bool
	TelegrafAddr  string
	TelegrafNet   string
	BufferSize    int
	customEmitter lineEmitter
}

type pipelineMonitor struct {
	cfg          Config
	emitter      lineEmitter
	runID        string
	runName      string
	origin       string
	pipelineName string
	baseTags     []string
	baseTagMap   map[string]string
	runOpts      model.RunOptions
	runOptsSet   bool
	uiOnce       sync.Once
	uiMu         sync.Mutex
	uiHub        *uiHub
	uiServer     *http.Server
	uiAddr       string
	uiErr        error
	uiMeta       []monitorEvent
	uiSeq        int64
	uiTotals     uiTotals
	uiRunStarted time.Time
}

// PipelineMonitorOption is the pipeline option returned by PipelineMonitor.
type PipelineMonitorOption = pipelineMonitor

// PipelineMonitor creates a monitoring pipeline option.
// Pass nil to use defaults.
func PipelineMonitor(cfg *Config) *PipelineMonitorOption {
	if cfg == nil {
		cfg = &Config{}
	}

	return &pipelineMonitor{cfg: *cfg}
}

// New initialises the monitoring option.
func (pm *pipelineMonitor) New() error {
	pm.applyDefaults()

	if pm.cfg.customEmitter != nil {
		pm.emitter = pm.cfg.customEmitter
	} else {
		emitter, err := newTelegrafEmitter(pm.cfg.TelegrafNet, pm.cfg.TelegrafAddr, pm.cfg.BufferSize)
		if err != nil {
			return fmt.Errorf("create telegraf emitter: %w", err)
		}

		pm.emitter = emitter
	}

	pm.buildBaseTags()

	return nil
}

// Finish stops the emitter.
func (pm *pipelineMonitor) Finish() error {
	err := pm.stopUI()
	if err != nil {
		return err
	}

	if pm.emitter == nil {
		return nil
	}

	err = pm.emitter.Close()
	if err != nil {
		return fmt.Errorf("close monitoring emitter: %w", err)
	}

	emitErr := pm.emitter.Err()
	if emitErr != nil {
		return fmt.Errorf("monitoring emitter error: %w", emitErr)
	}

	err = pm.uiError()
	if err != nil {
		return fmt.Errorf("monitoring ui: %w", err)
	}

	return nil
}

// SetRunOptions captures run settings.
func (pm *pipelineMonitor) SetRunOptions(opts model.RunOptions) {
	pm.runOpts = opts
	pm.runOptsSet = true

	if opts.DryRun {
		return
	}

	if pm.cfg.EnableUI {
		pm.uiMu.Lock()

		if pm.uiRunStarted.IsZero() {
			pm.uiRunStarted = time.Now()
		}

		pm.uiMu.Unlock()
	}

	if pm.cfg.EnableUI {
		pm.startUI()
	}
}

// PrepareStep records step metadata.
func (pm *pipelineMonitor) PrepareStep(parentStep, step *pipeline.StepInfo) error {
	pm.emitStepMeta(step, parentStep)
	pm.emitLink(parentStep, step)

	return nil
}

// PrepareSplitter records splitter metadata.
func (pm *pipelineMonitor) PrepareSplitter(parentStep, splitterStep *pipeline.StepInfo) error {
	pm.emitStepMeta(splitterStep, parentStep)
	pm.emitLink(parentStep, splitterStep)

	return nil
}

// PrepareMerger records merger metadata.
func (pm *pipelineMonitor) PrepareMerger(parentStep []*pipeline.StepInfo, step *pipeline.StepInfo) error {
	pm.emitStepMeta(step, nil)

	for _, parent := range parentStep {
		pm.emitLink(parent, step)
	}

	return nil
}

// PrepareSink records sink metadata.
func (pm *pipelineMonitor) PrepareSink(parentStep, step *pipeline.StepInfo) error {
	pm.emitStepMeta(step, parentStep)
	pm.emitLink(parentStep, step)

	return nil
}

// OnStepOutput records per-step metrics.
func (pm *pipelineMonitor) OnStepOutput(_, _ *pipeline.StepInfo) error {
	return nil
}

// OnSplitterOutput records per-splitter metrics.
func (pm *pipelineMonitor) OnSplitterOutput(_, _ *pipeline.StepInfo) error {
	return nil
}

// OnMergerOutput records per-merger metrics.
func (pm *pipelineMonitor) OnMergerOutput(_, _ *pipeline.StepInfo) error {
	return nil
}

// OnSinkOutput records per-sink metrics.
func (pm *pipelineMonitor) OnSinkOutput(_, _ *pipeline.StepInfo) error {
	return nil
}

// AfterSink records total duration for the sink.
func (pm *pipelineMonitor) AfterSink(_ *pipeline.StepInfo) error {
	return nil
}

// OnStepOutputMetrics records per-step metrics.
//
//nolint:unparam // Required by the pipeline option interface.
func (pm *pipelineMonitor) OnStepOutputMetrics(
	parentStep, step *pipeline.StepInfo,
	iterationDuration, computationDuration time.Duration,
) error {
	pm.emitOutput("step_output", parentStep, step, iterationDuration, computationDuration)

	return nil
}

// OnSplitterOutputMetrics records per-splitter metrics.
//
//nolint:unparam // Required by the pipeline option interface.
func (pm *pipelineMonitor) OnSplitterOutputMetrics(
	parentStep, splitterStep *pipeline.StepInfo,
	iterationDuration, computationDuration time.Duration,
) error {
	pm.emitOutput("splitter_output", parentStep, splitterStep, iterationDuration, computationDuration)

	return nil
}

// OnMergerOutputMetrics records per-merger metrics.
//
//nolint:unparam // Required by the pipeline option interface.
func (pm *pipelineMonitor) OnMergerOutputMetrics(
	parentStep, outputStep *pipeline.StepInfo,
	iterationDuration time.Duration,
) error {
	pm.emitOutput("merger_output", parentStep, outputStep, iterationDuration, 0)

	return nil
}

// OnSinkOutputMetrics records per-sink metrics.
//
//nolint:unparam // Required by the pipeline option interface.
func (pm *pipelineMonitor) OnSinkOutputMetrics(
	parentStep, step *pipeline.StepInfo,
	iterationDuration, computationDuration time.Duration,
) error {
	pm.emitOutput("sink_output", parentStep, step, iterationDuration, computationDuration)

	return nil
}

// AfterSinkMetrics records total duration for the sink.
//
//nolint:unparam // Required by the pipeline option interface.
func (pm *pipelineMonitor) AfterSinkMetrics(step *pipeline.StepInfo, totalDuration time.Duration) error {
	pm.emitFields(
		"run_total",
		pm.stepTags(step, nil),
		map[string]any{
			"duration_ms": durationMs(totalDuration),
		},
	)

	return nil
}

// OnStepDrop records drop events.
//
//nolint:unparam // interface requires error return.
func (pm *pipelineMonitor) OnStepDrop(step *pipeline.StepInfo, kind model.StepDropKind) error {
	pm.emitFields(
		"step_drop",
		pm.stepTags(step, nil, map[string]string{"drop_kind": string(kind)}),
		map[string]any{"count": int64(1)},
	)

	return nil
}

// OnStepRetry records retry events.
//
//nolint:unparam // interface requires error return.
func (pm *pipelineMonitor) OnStepRetry(_, step *pipeline.StepInfo, attempt int, duration time.Duration) error {
	pm.emitFields(
		"step_retry",
		pm.stepTags(step, nil),
		map[string]any{
			"count":       int64(1),
			"attempt":     int64(attempt),
			"duration_ms": durationMs(duration),
		},
	)

	return nil
}

// OnStepErrorRoute records error routing.
//
//nolint:unparam // interface requires error return.
func (pm *pipelineMonitor) OnStepErrorRoute(step *pipeline.StepInfo) error {
	pm.emitFields(
		"step_error_route",
		pm.stepTags(step, nil),
		map[string]any{"count": int64(1)},
	)

	return nil
}

func (pm *pipelineMonitor) emitStepMeta(step, parent *pipeline.StepInfo) {
	if step == nil {
		return
	}

	fields := map[string]any{
		"concurrent":  int64(step.Concurrent),
		"buffer_size": int64(step.BufferSize),
	}
	pm.emitFields("pipeline_step", pm.stepTags(step, parent), fields)
}

func (pm *pipelineMonitor) emitLink(from, to *pipeline.StepInfo) {
	if from == nil || to == nil {
		return
	}

	pm.emitFields(
		"pipeline_link",
		pm.stepTags(to, from, map[string]string{
			"from_step": from.Name,
			"to_step":   to.Name,
		}),
		map[string]any{"count": int64(1)},
	)
}

func (pm *pipelineMonitor) emitOutput(
	measurement string,
	parentStep, step *pipeline.StepInfo,
	iterationDuration, computationDuration time.Duration,
) {
	fields := map[string]any{
		"count":        int64(1),
		"transport_ms": durationMs(iterationDuration),
		"duration_ms":  durationMs(computationDuration),
	}
	pm.emitFields(measurement, pm.stepTags(step, parentStep), fields)
}

func (pm *pipelineMonitor) emitFields(measurement string, tags map[string]string, fields map[string]any) {
	if pm == nil || pm.runOpts.DryRun {
		return
	}

	now := time.Now()

	if pm.emitter != nil {
		line := buildLine(measurement, pm.baseTags, tags, fields, now)
		if line != "" {
			pm.emitter.Emit(line)
		}
	}

	pm.emitUI(measurement, tags, fields, now)
}

func (pm *pipelineMonitor) applyDefaults() {
	if pm.cfg.TelegrafNet == "" {
		pm.cfg.TelegrafNet = defaultTelegrafNetwork
	}

	if pm.cfg.TelegrafAddr == "" {
		pm.cfg.TelegrafAddr = defaultTelegrafAddress
	}

	if pm.cfg.BufferSize < 1 {
		pm.cfg.BufferSize = defaultBufferSize
	}

	if pm.cfg.EnableUI && pm.cfg.BindAddr == "" {
		pm.cfg.BindAddr = defaultUIBindAddr
	}

	if pm.cfg.RunID == "" {
		pm.cfg.RunID = newRunID()
	}

	pm.runID = pm.cfg.RunID

	if pm.cfg.RunName == "" {
		pm.cfg.RunName = pm.cfg.PipelineName
	}

	if pm.cfg.RunName == "" {
		pm.cfg.RunName = pm.runID
	}

	pm.runName = pm.cfg.RunName

	if pm.cfg.Origin == "" {
		pm.cfg.Origin = defaultOrigin()
	}

	pm.origin = pm.cfg.Origin
	pm.pipelineName = pm.cfg.PipelineName
}

func (pm *pipelineMonitor) buildBaseTags() {
	pm.baseTagMap = map[string]string{
		"run_id":   pm.runID,
		"run_name": pm.runName,
		"origin":   pm.origin,
	}

	if pm.pipelineName != "" {
		pm.baseTagMap["pipeline_name"] = pm.pipelineName
	}

	pm.baseTags = make([]string, 0, baseTagCapacity)
	pm.baseTags = append(
		pm.baseTags,
		tagPair("run_id", pm.runID),
		tagPair("run_name", pm.runName),
		tagPair("origin", pm.origin),
	)

	if pm.pipelineName != "" {
		pm.baseTags = append(pm.baseTags, tagPair("pipeline_name", pm.pipelineName))
	}
}

func (pm *pipelineMonitor) stepTags(
	step, parent *pipeline.StepInfo,
	extra ...map[string]string,
) map[string]string {
	tags := map[string]string{}

	if step != nil {
		if step.Name != "" {
			tags["step_name"] = step.Name
		}

		if step.Type != "" {
			tags["step_type"] = string(step.Type)
		}
	}

	if parent != nil && parent.Name != "" {
		tags["parent_step"] = parent.Name
	}

	for _, extraTags := range extra {
		for key, value := range extraTags {
			if value == "" {
				continue
			}

			tags[key] = value
		}
	}

	return tags
}

func durationMs(duration time.Duration) int64 {
	if duration <= 0 {
		return 0
	}

	return duration.Milliseconds()
}

func defaultOrigin() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}

	return host + ":" + strconv.Itoa(os.Getpid())
}

func newRunID() string {
	var buf [16]byte

	_, err := rand.Read(buf[:])
	if err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}

	return hex.EncodeToString(buf[:])
}

type lineEmitter interface {
	Emit(line string)
	Close() error
	Err() error
}

type telegrafEmitter struct {
	conn net.Conn
	ch   chan string
	done chan struct{}
	mu   sync.Mutex
	err  error
}

func newTelegrafEmitter(network, address string, bufferSize int) (*telegrafEmitter, error) {
	dialer := net.Dialer{}

	conn, err := dialer.DialContext(context.Background(), network, address)
	if err != nil {
		return nil, fmt.Errorf("dial telegraf: %w", err)
	}

	emitter := &telegrafEmitter{
		conn: conn,
		ch:   make(chan string, bufferSize),
		done: make(chan struct{}),
	}

	go emitter.run()

	return emitter, nil
}

func (te *telegrafEmitter) Emit(line string) {
	if te == nil || line == "" {
		return
	}

	select {
	case te.ch <- line:
	default:
	}
}

func (te *telegrafEmitter) Close() error {
	if te == nil {
		return nil
	}

	close(te.ch)
	<-te.done

	err := te.conn.Close()
	if err != nil {
		return fmt.Errorf("close telegraf connection: %w", err)
	}

	return nil
}

func (te *telegrafEmitter) Err() error {
	te.mu.Lock()
	defer te.mu.Unlock()

	return te.err
}

func (te *telegrafEmitter) run() {
	defer close(te.done)

	for line := range te.ch {
		if line == "" {
			continue
		}

		_, err := te.conn.Write([]byte(line + "\n"))
		if err != nil {
			te.setErr(err)
		}
	}
}

func (te *telegrafEmitter) setErr(err error) {
	if err == nil {
		return
	}

	te.mu.Lock()
	defer te.mu.Unlock()

	if te.err == nil {
		te.err = err
	}
}

func buildLine(
	measurement string,
	baseTags []string,
	tags map[string]string,
	fields map[string]any,
	ts time.Time,
) string {
	if measurement == "" || len(fields) == 0 {
		return ""
	}

	tagStr := formatTags(baseTags, tags)

	fieldStr := formatFields(fields)
	if fieldStr == "" {
		return ""
	}

	var builder strings.Builder
	builder.Grow(defaultLineBuilderSize)
	builder.WriteString(escapeToken(measurement))

	if tagStr != "" {
		builder.WriteByte(',')
		builder.WriteString(tagStr)
	}

	builder.WriteByte(' ')
	builder.WriteString(fieldStr)
	builder.WriteByte(' ')
	builder.WriteString(strconv.FormatInt(ts.UnixNano(), 10))

	return builder.String()
}

func formatTags(base []string, extra map[string]string) string {
	if len(base) == 0 && len(extra) == 0 {
		return ""
	}

	tags := make([]string, 0, len(base)+len(extra))
	tags = append(tags, base...)

	if len(extra) > 0 {
		keys := make([]string, 0, len(extra))

		for key := range extra {
			keys = append(keys, key)
		}

		sort.Strings(keys)

		for _, key := range keys {
			value := extra[key]
			if value == "" {
				continue
			}

			tags = append(tags, tagPair(key, value))
		}
	}

	return strings.Join(tags, ",")
}

func formatFields(fields map[string]any) string {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	var builder strings.Builder
	written := false

	for _, key := range keys {
		value := fields[key]
		if value == nil {
			continue
		}

		if written {
			builder.WriteByte(',')
		}

		builder.WriteString(escapeToken(key))
		builder.WriteByte('=')
		builder.WriteString(formatFieldValue(value))

		written = true
	}

	return builder.String()
}

func formatFieldValue(value any) string {
	switch val := value.(type) {
	case int:
		return strconv.FormatInt(int64(val), 10) + "i"
	case int64:
		return strconv.FormatInt(val, 10) + "i"
	case int32:
		return strconv.FormatInt(int64(val), 10) + "i"
	case uint:
		return strconv.FormatUint(uint64(val), 10) + "i"
	case uint64:
		return strconv.FormatUint(val, 10) + "i"
	case uint32:
		return strconv.FormatUint(uint64(val), 10) + "i"
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		if val {
			return "true"
		}

		return "false"
	case string:
		return `"` + escapeFieldString(val) + `"`
	default:
		return `"` + escapeFieldString(fmt.Sprint(val)) + `"`
	}
}

func escapeToken(value string) string {
	value = strings.ReplaceAll(value, ",", "\\,")
	value = strings.ReplaceAll(value, " ", "\\ ")
	value = strings.ReplaceAll(value, "=", "\\=")

	return value
}

func escapeFieldString(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\"", "\\\"")

	return value
}

func tagPair(key, value string) string {
	return escapeToken(key) + "=" + escapeToken(value)
}
