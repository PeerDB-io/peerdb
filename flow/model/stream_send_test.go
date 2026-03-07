package model

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQRecordStreamSendReturnsCanceledWhenBufferFull(t *testing.T) {
	t.Parallel()

	stream := NewQRecordStream(1)
	require.NoError(t, stream.Send(context.Background(), nil))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := stream.Send(ctx, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestQObjectStreamSendReturnsCanceledWhenBufferFull(t *testing.T) {
	t.Parallel()

	stream := NewQObjectStream(1)
	require.NoError(t, stream.Send(context.Background(), &Object{URL: "u", Size: 1}))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := stream.Send(ctx, &Object{URL: "v", Size: 2})
	require.ErrorIs(t, err, context.Canceled)
}
