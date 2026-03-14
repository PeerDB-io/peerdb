package connpostgres

import (
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"
)

// TestBaseRecordV2 covers the two cases the v2 DML dispatch hits:
// streaming (commitLock nil, wrapper Xid set) and non-streaming v2
// (commitLock from Begin, wrapper Xid zero so we fall back to it).
func TestBaseRecordV2(t *testing.T) {
	t.Run("streaming uses wrapper Xid", func(t *testing.T) {
		p := &PostgresCDCSource{}
		rec := p.baseRecordV2(pglogrepl.LSN(0xdeadbeef), 12345)
		require.Equal(t, int64(0xdeadbeef), rec.CheckpointID)
		require.Equal(t, uint64(12345), rec.TransactionID)
		require.Equal(t, int64(0), rec.CommitTimeNano)
	})

	t.Run("non-streaming falls back to commitLock Xid", func(t *testing.T) {
		p := &PostgresCDCSource{
			commitLock: &pglogrepl.BeginMessage{Xid: 67890},
		}
		rec := p.baseRecordV2(pglogrepl.LSN(42), 0)
		require.Equal(t, int64(42), rec.CheckpointID)
		require.Equal(t, uint64(67890), rec.TransactionID, "should fall back to commitLock.Xid when wrapper Xid is 0")
	})

	t.Run("wrapper Xid wins over commitLock when both present", func(t *testing.T) {
		// shouldn't happen in practice (StreamStartMessageV2 implies no Begin in flight),
		// but the precedence matters: wrapper carries the streaming subtransaction XID.
		p := &PostgresCDCSource{
			commitLock: &pglogrepl.BeginMessage{Xid: 67890},
		}
		rec := p.baseRecordV2(pglogrepl.LSN(42), 12345)
		require.Equal(t, uint64(12345), rec.TransactionID)
	})
}
