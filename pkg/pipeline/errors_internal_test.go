package pipeline

import (
	"errors"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorChans(t *testing.T) {
	t.Parallel()

	ecs := errorChans{}
	ec1 := &errorChan{}
	ec2 := &errorChan{}
	doneChan := make(chan struct{}, 2)

	go func() {
		ecs.add(ec1)

		doneChan <- struct{}{}
	}()

	go func() {
		ecs.add(ec2)

		doneChan <- struct{}{}
	}()

	<-doneChan
	<-doneChan
	assert.ElementsMatch(t, []*errorChan{ec1, ec2}, ecs.list)
}

func TestNewErrorChan(t *testing.T) {
	t.Parallel()

	ec1 := newErrorChan("error chan", nil)
	expectedEc1 := &errorChan{
		name: "error chan",
	}
	assert.Equal(t, expectedEc1, ec1)

	c2 := make(chan error)
	ec2 := newErrorChan("error chan 2", c2)
	expectedEc2 := &errorChan{
		name: "error chan 2",
		c:    c2,
	}
	assert.Equal(t, expectedEc2, ec2)
}

func TestMergeErrorsAllNil(t *testing.T) {
	t.Parallel()

	ec1 := newErrorChan("error chan", nil)
	ec2 := newErrorChan("error chan 2", nil)

	outErrorChan := mergeErrors(ec1, ec2)
	gotErr, open := <-outErrorChan
	assert.False(t, open)
	assert.NoError(t, gotErr)
}

var (
	err1 = errors.New("error 1")
	err2 = errors.New("error 2")
)

func TestMergeErrorsOneNil(t *testing.T) {
	t.Parallel()

	ec1 := newErrorChan("error chan", nil)
	chan2 := make(chan error)
	ec2 := newErrorChan("error chan 2", chan2)

	expectedError1 := err1
	expectedError2 := err2

	go func() {
		defer close(chan2)

		chan2 <- expectedError1

		chan2 <- expectedError2
	}()

	outErrorChan := mergeErrors(ec1, ec2)

	gotErrs := []error{}
	for err := range outErrorChan {
		gotErrs = append(gotErrs, err)
	}

	sort.Slice(gotErrs, func(i, j int) bool {
		return gotErrs[i].Error() < gotErrs[j].Error()
	})

	require.ErrorIs(t, gotErrs[0], expectedError1)
	require.ErrorIs(t, gotErrs[1], expectedError2)
}

func TestMergeErrors(t *testing.T) {
	t.Parallel()

	chan1 := make(chan error)
	ec1 := newErrorChan("error chan", chan1)
	chan2 := make(chan error)
	ec2 := newErrorChan("error chan", chan2)

	expectedError1 := err1
	expectedError2 := err2

	go func() {
		defer close(chan1)
		defer close(chan2)

		chan1 <- expectedError1

		chan2 <- expectedError2
	}()

	outErrorChan := mergeErrors(ec1, ec2)

	gotErrs := []error{}
	for err := range outErrorChan {
		gotErrs = append(gotErrs, err)
	}

	sort.Slice(gotErrs, func(i, j int) bool {
		return gotErrs[i].Error() < gotErrs[j].Error()
	})

	require.ErrorIs(t, gotErrs[0], expectedError1)
	require.ErrorIs(t, gotErrs[1], expectedError2)
}
