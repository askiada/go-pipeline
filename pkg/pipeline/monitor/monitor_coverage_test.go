//nolint:testpackage // exercises internal monitor helpers directly.
package monitor

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline"
)

type errEmitter struct {
	closeErr error
	err      error
}

func (e errEmitter) Emit(string) {}

func (e errEmitter) Close() error {
	return e.closeErr
}

func (e errEmitter) Err() error {
	return e.err
}

type stubAddr struct{}

func (stubAddr) Network() string { return "stub" }
func (stubAddr) String() string  { return "stub" }

type stubConn struct {
	writeErr error
	closeErr error
}

func (c *stubConn) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (c *stubConn) Write(data []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}

	return len(data), nil
}

func (c *stubConn) Close() error {
	return c.closeErr
}

func (c *stubConn) LocalAddr() net.Addr {
	return stubAddr{}
}

func (c *stubConn) RemoteAddr() net.Addr {
	return stubAddr{}
}

func (c *stubConn) SetDeadline(time.Time) error {
	return nil
}

func (c *stubConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *stubConn) SetWriteDeadline(time.Time) error {
	return nil
}

func TestPipelineMonitorNilConfig(t *testing.T) {
	t.Parallel()

	opt := PipelineMonitor(nil)
	require.NotNil(t, opt)
}

func TestPipelineMonitorNewErrorBadNetwork(t *testing.T) {
	t.Parallel()

	opt := PipelineMonitor(&Config{
		TelegrafNet:  "bad-network",
		TelegrafAddr: "bad-addr",
	})
	require.Error(t, opt.New())
}

func TestPipelineMonitorNewWithTelegrafEmitter(t *testing.T) {
	t.Parallel()

	listener, err := (&net.ListenConfig{}).ListenPacket(context.Background(), "udp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("udp listener unavailable: %v", err)
	}
	defer listener.Close()

	opt := PipelineMonitor(&Config{
		TelegrafNet:  "udp",
		TelegrafAddr: listener.LocalAddr().String(),
		BufferSize:   1,
	})
	require.NoError(t, opt.New())
	require.NoError(t, opt.Finish())
}

func TestPipelineMonitorFinishEmitterErrors(t *testing.T) {
	t.Parallel()

	closeErr := errors.New("close failed")
	opt := PipelineMonitor(&Config{customEmitter: errEmitter{closeErr: closeErr}})
	require.NoError(t, opt.New())
	require.ErrorContains(t, opt.Finish(), "close monitoring emitter")

	emitErr := errors.New("emit failed")
	opt = PipelineMonitor(&Config{customEmitter: errEmitter{err: emitErr}})
	require.NoError(t, opt.New())
	require.ErrorContains(t, opt.Finish(), "monitoring emitter error")
}

func TestPipelineMonitorFinishUIError(t *testing.T) {
	t.Parallel()

	opt := PipelineMonitor(&Config{customEmitter: errEmitter{}})
	require.NoError(t, opt.New())

	opt.setUIErr(errors.New("ui failed"))
	require.ErrorContains(t, opt.Finish(), "monitoring ui")
}

func TestMonitorEmitStepMetaAndLinkNil(t *testing.T) {
	t.Parallel()

	emitter := &stubEmitter{}
	opt := PipelineMonitor(&Config{customEmitter: emitter})
	require.NoError(t, opt.New())

	opt.emitStepMeta(nil, nil)
	opt.emitLink(nil, &pipeline.StepInfo{})
	opt.emitLink(&pipeline.StepInfo{}, nil)

	require.Empty(t, emitter.Lines())
}

func TestMonitorStepTagsSkipsEmptyValues(t *testing.T) {
	t.Parallel()

	opt := PipelineMonitor(&Config{customEmitter: &stubEmitter{}})
	require.NoError(t, opt.New())

	tags := opt.stepTags(
		&pipeline.StepInfo{Name: "", Type: ""},
		&pipeline.StepInfo{Name: ""},
		map[string]string{
			"extra": "value",
			"empty": "",
		},
	)

	require.Equal(t, map[string]string{"extra": "value"}, tags)
}

func TestDefaultOrigin(t *testing.T) {
	origin := defaultOrigin()
	require.Contains(t, origin, ":")
}

func TestTelegrafEmitterEmitDropsWhenFull(t *testing.T) {
	t.Parallel()

	emitter := &telegrafEmitter{ch: make(chan string, 1)}
	emitter.Emit("first")
	emitter.Emit("second")
	emitter.Emit("")

	require.Len(t, emitter.ch, 1)

	<-emitter.ch
}

func TestTelegrafEmitterRunSetsError(t *testing.T) {
	t.Parallel()

	writeErr := errors.New("write failed")
	emitter := &telegrafEmitter{
		conn: &stubConn{writeErr: writeErr},
		ch:   make(chan string, 1),
		done: make(chan struct{}),
	}

	go emitter.run()

	emitter.Emit("line")
	close(emitter.ch)
	<-emitter.done

	require.ErrorIs(t, emitter.Err(), writeErr)
}

func TestTelegrafEmitterCloseError(t *testing.T) {
	t.Parallel()

	emitter := &telegrafEmitter{
		conn: &stubConn{closeErr: errors.New("close failed")},
		ch:   make(chan string),
		done: make(chan struct{}),
	}

	go emitter.run()

	err := emitter.Close()
	require.ErrorContains(t, err, "close telegraf connection")
}

func TestTelegrafEmitterNilGuards(t *testing.T) {
	t.Parallel()

	var emitter *telegrafEmitter
	emitter.Emit("line")
	require.NoError(t, emitter.Close())
}

func TestFormatHelpersCoverTypes(t *testing.T) {
	t.Parallel()

	require.Equal(t, "1i", formatFieldValue(int(1)))
	require.Equal(t, "2i", formatFieldValue(int64(2)))
	require.Equal(t, "3i", formatFieldValue(int32(3)))
	require.Equal(t, "4i", formatFieldValue(uint(4)))
	require.Equal(t, "5i", formatFieldValue(uint64(5)))
	require.Equal(t, "6i", formatFieldValue(uint32(6)))
	require.Equal(t, "1.25", formatFieldValue(float32(1.25)))
	require.Equal(t, "2.5", formatFieldValue(float64(2.5)))
	require.Equal(t, "true", formatFieldValue(true))
	require.Equal(t, "false", formatFieldValue(false))
	require.Equal(t, "\"value\"", formatFieldValue("value"))
	require.Equal(t, "\"{1}\"", formatFieldValue(struct{ A int }{A: 1}))

	require.Equal(t, "base=1,extra=2", formatTags([]string{"base=1"}, map[string]string{"extra": "2", "skip": ""}))
	require.Empty(t, formatTags(nil, nil))

	require.Equal(t, "b=1i", formatFields(map[string]any{"a": nil, "b": int64(1)}))
}

func TestEmitUIDisabled(t *testing.T) {
	t.Parallel()

	var pm *pipelineMonitor
	pm.emitUI("event", nil, map[string]any{"count": int64(1)}, time.Now())

	opt := PipelineMonitor(&Config{EnableUI: false, customEmitter: &stubEmitter{}})
	require.NoError(t, opt.New())
	opt.emitUI("event", nil, map[string]any{"count": int64(1)}, time.Now())
}

func TestStartUIBindError(t *testing.T) {
	t.Parallel()

	opt := PipelineMonitor(&Config{
		EnableUI:      true,
		BindAddr:      "127.0.0.1:bad",
		customEmitter: &stubEmitter{},
	})
	require.NoError(t, opt.New())

	opt.startUI()
	require.Error(t, opt.uiError())
}

func TestStartStopUISucceeds(t *testing.T) {
	t.Parallel()

	opt := PipelineMonitor(&Config{
		EnableUI:      true,
		BindAddr:      "127.0.0.1:0",
		customEmitter: &stubEmitter{},
	})
	require.NoError(t, opt.New())

	opt.startUI()
	require.NoError(t, opt.stopUI())
}
