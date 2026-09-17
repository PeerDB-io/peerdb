package peerflow

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestResolveResyncTableMappings(t *testing.T) {
	current := []*protos.TableMapping{
		{
			SourceTableIdentifier:      "public.orders",
			DestinationTableIdentifier: "analytics.orders",
			Exclude:                    []string{"secret"},
		},
		{
			SourceTableIdentifier:      "public.customers",
			DestinationTableIdentifier: "analytics.customers",
		},
	}

	t.Run("returns requested replacement settings", func(t *testing.T) {
		resolved, err := resolveResyncTableMappings(current, []*protos.TableMapping{{
			SourceTableIdentifier:      "public.orders",
			DestinationTableIdentifier: "analytics.orders",
			PartitionKey:               "toYYYYMM(created_at)",
		}})
		require.NoError(t, err)
		require.Len(t, resolved, 1)
		require.Equal(t, "toYYYYMM(created_at)", resolved[0].PartitionKey)
		require.Empty(t, resolved[0].Exclude)
		require.NotSame(t, current[0], resolved[0])
	})

	t.Run("requires destination identity", func(t *testing.T) {
		_, err := resolveResyncTableMappings(current, []*protos.TableMapping{{
			SourceTableIdentifier: "public.customers",
		}})
		require.EqualError(t, err, "destination for table public.customers does not match the mirror configuration")
	})

	t.Run("rejects table outside mirror", func(t *testing.T) {
		_, err := resolveResyncTableMappings(current, []*protos.TableMapping{{
			SourceTableIdentifier: "public.missing",
		}})
		require.EqualError(t, err, "table public.missing is not part of the mirror")
	})

	t.Run("rejects destination mismatch", func(t *testing.T) {
		_, err := resolveResyncTableMappings(current, []*protos.TableMapping{{
			SourceTableIdentifier:      "public.orders",
			DestinationTableIdentifier: "analytics.other",
		}})
		require.EqualError(t, err, "destination for table public.orders does not match the mirror configuration")
	})

	t.Run("rejects duplicates", func(t *testing.T) {
		_, err := resolveResyncTableMappings(current, []*protos.TableMapping{
			{SourceTableIdentifier: "public.orders", DestinationTableIdentifier: "analytics.orders"},
			{SourceTableIdentifier: "public.orders", DestinationTableIdentifier: "analytics.orders"},
		})
		require.EqualError(t, err, "table public.orders was requested for resync more than once")
	})
}

func TestValidateResyncTableUpdate(t *testing.T) {
	resync := []*protos.TableMapping{{SourceTableIdentifier: "public.orders"}}
	require.NoError(t, validateResyncTableUpdate(resync,
		[]*protos.TableMapping{{SourceTableIdentifier: "public.new"}},
		[]*protos.TableMapping{{SourceTableIdentifier: "public.old"}}))

	err := validateResyncTableUpdate(resync,
		[]*protos.TableMapping{{SourceTableIdentifier: "public.orders"}}, nil)
	require.EqualError(t, err, "table public.orders cannot be added and resynced in the same update")

	err = validateResyncTableUpdate(resync, nil,
		[]*protos.TableMapping{{SourceTableIdentifier: "public.orders"}})
	require.EqualError(t, err, "table public.orders cannot be removed and resynced in the same update")
}

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
