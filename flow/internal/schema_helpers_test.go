package internal

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
		resolved, err := ResolveResyncTableMappings(current, []*protos.TableMapping{{
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
		_, err := ResolveResyncTableMappings(current, []*protos.TableMapping{{
			SourceTableIdentifier: "public.customers",
		}})
		require.EqualError(t, err, "destination for table public.customers does not match the mirror configuration")
	})

	t.Run("rejects table outside mirror", func(t *testing.T) {
		_, err := ResolveResyncTableMappings(current, []*protos.TableMapping{{
			SourceTableIdentifier: "public.missing",
		}})
		require.EqualError(t, err, "table public.missing is not part of the mirror")
	})

	t.Run("rejects destination mismatch", func(t *testing.T) {
		_, err := ResolveResyncTableMappings(current, []*protos.TableMapping{{
			SourceTableIdentifier:      "public.orders",
			DestinationTableIdentifier: "analytics.other",
		}})
		require.EqualError(t, err, "destination for table public.orders does not match the mirror configuration")
	})

	t.Run("rejects duplicates", func(t *testing.T) {
		_, err := ResolveResyncTableMappings(current, []*protos.TableMapping{
			{SourceTableIdentifier: "public.orders", DestinationTableIdentifier: "analytics.orders"},
			{SourceTableIdentifier: "public.orders", DestinationTableIdentifier: "analytics.orders"},
		})
		require.EqualError(t, err, "table public.orders was requested for resync more than once")
	})
}

func TestValidateResyncTableUpdate(t *testing.T) {
	resync := []*protos.TableMapping{{SourceTableIdentifier: "public.orders"}}
	require.NoError(t, ValidateResyncTableUpdate(resync,
		[]*protos.TableMapping{{SourceTableIdentifier: "public.new"}},
		[]*protos.TableMapping{{SourceTableIdentifier: "public.old"}}))

	err := ValidateResyncTableUpdate(resync,
		[]*protos.TableMapping{{SourceTableIdentifier: "public.orders"}}, nil)
	require.EqualError(t, err, "table public.orders cannot be added and resynced in the same update")

	err = ValidateResyncTableUpdate(resync, nil,
		[]*protos.TableMapping{{SourceTableIdentifier: "public.orders"}})
	require.EqualError(t, err, "table public.orders cannot be removed and resynced in the same update")
}
