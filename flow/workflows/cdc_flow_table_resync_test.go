package peerflow

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestApplyResyncedTableMappings(t *testing.T) {
	unchanged := &protos.TableMapping{
		SourceTableIdentifier:      "public.customers",
		DestinationTableIdentifier: "analytics.customers",
	}
	replacement := &protos.TableMapping{
		SourceTableIdentifier:      "public.orders",
		DestinationTableIdentifier: "analytics.orders",
		Columns: []*protos.ColumnSetting{
			{SourceName: "customer_id", Ordering: 1},
			{SourceName: "id", Ordering: 2},
		},
	}
	options := &protos.SyncFlowOptions{
		TableMappings: []*protos.TableMapping{
			{SourceTableIdentifier: "public.orders", DestinationTableIdentifier: "analytics.orders"},
			unchanged,
		},
		SrcTableIdNameMapping: map[uint32]string{
			10: "public.orders",
			20: "public.customers",
		},
	}
	result := &CDCFlowWorkflowResult{SyncFlowOptions: &protos.SyncFlowOptions{
		SrcTableIdNameMapping: map[uint32]string{30: "public.orders"},
	}}

	applyResyncedTableMappings(options, []*protos.TableMapping{replacement}, result)

	require.Equal(t, replacement.Columns, options.TableMappings[0].Columns)
	require.NotSame(t, replacement, options.TableMappings[0])
	require.Same(t, unchanged, options.TableMappings[1])
	require.Equal(t, map[uint32]string{
		20: "public.customers",
		30: "public.orders",
	}, options.SrcTableIdNameMapping)
}
