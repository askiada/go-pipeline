package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type rootPrepareObserver struct {
	PipelineDefaults

	err error
}

func (o rootPrepareObserver) PrepareStep(_, _ *StepInfo) error {
	return o.err
}

func TestRootPrepareStepError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("prepare failed")
	pipe, err := New(rootPrepareObserver{err: expectedErr})
	require.NoError(t, err)

	root := Root(pipe, "root", func(context.Context, chan<- int) error {
		return nil
	})
	require.Nil(t, root)
	require.ErrorIs(t, pipe.Err(), expectedErr)
}

func TestRootBuildErrReturnsNil(t *testing.T) {
	t.Parallel()

	pipe := &Pipeline{buildErr: errors.New("build failed")}
	root := Root(pipe, "root", func(context.Context, chan<- int) error {
		return nil
	})
	require.Nil(t, root)
}
