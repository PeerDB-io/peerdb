package connpostgres

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPgCopySyncErrorReachesSource(t *testing.T) {
	t.Parallel()
	reader, writer := NewPgCopyPipe()
	defer reader.Close()
	defer writer.Close(nil)

	syncErr := errors.New("destination copy failed")
	done := make(chan error, 1)
	go func() {
		_, err := writer.Write([]byte("1\n"))
		done <- err
	}()

	writer.HandleQRepSyncError(syncErr)
	require.ErrorIs(t, <-done, syncErr)
}
