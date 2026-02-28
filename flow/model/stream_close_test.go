package model

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQRecordStream_CloseIsIdempotent(t *testing.T) {
	stream := NewQRecordStream(0)
	secondErr := errors.New("second close")

	require.NotPanics(t, func() {
		stream.Close(nil)
		stream.Close(secondErr)
		stream.Close(nil)
	})

	_, ok := <-stream.Records
	require.False(t, ok)
	require.NoError(t, stream.Err())

	_, err := stream.Schema()
	require.NoError(t, err)
}

func TestQObjectStream_CloseIsIdempotent(t *testing.T) {
	stream := NewQObjectStream(0)
	firstErr := errors.New("first close")

	require.NotPanics(t, func() {
		stream.Close(firstErr)
		stream.Close(nil)
		stream.Close(errors.New("third close"))
	})

	_, ok := <-stream.Objects
	require.False(t, ok)
	require.ErrorIs(t, stream.Err(), firstErr)
}
