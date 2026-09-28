package connpostgres

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func TestCtidPartitionsForChildTablesOffsetNumberBounds(t *testing.T) {
	t.Parallel()
	pp := PartitionParams{
		numPartitions: 8,
		logger:        log.NewStructuredLogger(slog.With(slog.String(string(shared.FlowNameKey), "testOffsetBounds"))),
	}
	leafBlocks := map[string]tableBlockStats{
		"public.t1": {blockCount: 100},
		"public.t2": {blockCount: 45},
		"public.t3": {blockCount: 10},
	}
	// blocksPerPartition = DivCeil(155, 8) = 20
	partitions, err := ctidPartitionsForChildTables(pp, leafBlocks)
	require.NoError(t, err)
	require.Len(t, partitions, 8)

	childRange := func(table string, startBlock, endBlock uint32) *protos.ChildTableRange {
		return &protos.ChildTableRange{
			Table: table,
			Start: startBlock,
			End:   endBlock,
		}
	}
	expected := [][]*protos.ChildTableRange{
		{childRange("public.t1", 0, 19)},
		{childRange("public.t1", 20, 39)},
		{childRange("public.t1", 40, 59)},
		{childRange("public.t1", 60, 79)},
		{childRange("public.t1", 80, 99)},
		{childRange("public.t2", 0, 19)},
		{childRange("public.t2", 20, 39)},
		{childRange("public.t2", 40, 44), childRange("public.t3", 0, 9)},
	}

	for i, p := range partitions {
		require.Len(t, p.ChildTableRanges, len(expected[i]))
		for j, ctr := range p.ChildTableRanges {
			exp := expected[i][j]
			msg := fmt.Sprintf("partition %d range %d", i, j)
			assert.Equal(t, exp.Table, ctr.Table, msg)
			assert.Equal(t, exp.Start, ctr.Start, msg)
			assert.Equal(t, exp.End, ctr.End, msg)
		}
	}
}
