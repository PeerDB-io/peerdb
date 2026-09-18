package bigquery

import (
	"context"
	"errors"
	"io"
	"testing"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubStorageReadRows struct {
	response *storagepb.ReadRowsResponse
	err      error
	calls    int
}

func (s *stubStorageReadRows) Recv() (*storagepb.ReadRowsResponse, error) {
	s.calls++
	return s.response, s.err
}

func TestValidateStorageReadSession(t *testing.T) {
	const streamName = "projects/project/locations/location/sessions/session/streams/stream"
	session := &storagepb.ReadSession{
		Streams: []*storagepb.ReadStream{{Name: streamName}},
	}

	t.Run("reads first response", func(t *testing.T) {
		rows := &stubStorageReadRows{response: &storagepb.ReadRowsResponse{RowCount: 1}}
		err := validateStorageReadSession(t.Context(), session, func(
			_ context.Context, req *storagepb.ReadRowsRequest,
		) (storageReadRows, error) {
			assert.Equal(t, streamName, req.GetReadStream())
			return rows, nil
		})
		require.NoError(t, err)
		assert.Equal(t, 1, rows.calls)
	})

	t.Run("accepts empty stream", func(t *testing.T) {
		rows := &stubStorageReadRows{err: io.EOF}
		err := validateStorageReadSession(t.Context(), session, func(
			context.Context, *storagepb.ReadRowsRequest,
		) (storageReadRows, error) {
			return rows, nil
		})
		require.NoError(t, err)
		assert.Equal(t, 1, rows.calls)
	})

	t.Run("accepts session without streams", func(t *testing.T) {
		called := false
		err := validateStorageReadSession(t.Context(), &storagepb.ReadSession{}, func(
			context.Context, *storagepb.ReadRowsRequest,
		) (storageReadRows, error) {
			called = true
			return nil, nil
		})
		require.NoError(t, err)
		assert.False(t, called)
	})

	t.Run("returns stream creation error", func(t *testing.T) {
		wantErr := errors.New("missing readsessions.getData")
		err := validateStorageReadSession(t.Context(), session, func(
			context.Context, *storagepb.ReadRowsRequest,
		) (storageReadRows, error) {
			return nil, wantErr
		})
		require.ErrorIs(t, err, wantErr)
		assert.ErrorContains(t, err, "failed to open read stream")
	})

	t.Run("returns first receive error", func(t *testing.T) {
		wantErr := errors.New("read denied")
		rows := &stubStorageReadRows{err: wantErr}
		err := validateStorageReadSession(t.Context(), session, func(
			context.Context, *storagepb.ReadRowsRequest,
		) (storageReadRows, error) {
			return rows, nil
		})
		require.ErrorIs(t, err, wantErr)
		assert.ErrorContains(t, err, "failed to read from stream")
	})
}
