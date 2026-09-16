package db

import (
	"hash/crc32"
	"testing"

	"github.com/pressly/goose/v3/lock"
	"github.com/stretchr/testify/require"
)

func TestBootstrapLockIDIsPinned(t *testing.T) {
	require.Equal(t, bootstrapLockID, int64(crc32.ChecksumIEEE([]byte("peerdb"))))
	require.NotEqual(t, lock.DefaultLockID, bootstrapLockID)
}
