package cmd

import (
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestQRepPartitionStatusQueryIsBounded(t *testing.T) {
	t.Parallel()

	require.Equal(t, 10000, qrepPartitionStatusLimit)
	require.Contains(t, qrepPartitionStatusQuery, "flow_name=$1")
	require.Contains(t, qrepPartitionStatusQuery, "ORDER BY start_time DESC NULLS LAST")
	require.Contains(t, qrepPartitionStatusQuery, "id DESC")
	require.Contains(t, qrepPartitionStatusQuery, fmt.Sprintf("LIMIT %d", qrepPartitionStatusLimit))

	partition := &protos.PartitionStatus{
		PartitionId:     "11111111-1111-1111-1111-111111111111",
		StartTime:       timestamppb.New(time.Unix(1_700_000_000, 1).UTC()),
		EndTime:         timestamppb.New(time.Unix(1_700_000_001, 1).UTC()),
		RowsInPartition: math.MaxInt64,
		RowsSynced:      math.MaxInt64,
	}
	require.Len(t, partition.PartitionId, 36)

	const grpcDefaultMaxReceiveBytes = 4194304
	capped := &protos.QRepMirrorStatus{
		Partitions: slices.Repeat([]*protos.PartitionStatus{partition}, qrepPartitionStatusLimit),
	}
	require.Less(t, proto.Size(capped), grpcDefaultMaxReceiveBytes)

	unbounded := &protos.QRepMirrorStatus{
		Partitions: slices.Repeat([]*protos.PartitionStatus{partition}, 164000),
	}
	require.GreaterOrEqual(t, proto.Size(unbounded), grpcDefaultMaxReceiveBytes)
}
