package internal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestValidateResyncTableMappings(t *testing.T) {
	current := []*protos.TableMapping{
		{
			SourceTableIdentifier:      "public.orders",
			DestinationTableIdentifier: "analytics.orders",
		},
		{
			SourceTableIdentifier:      "public.customers",
			DestinationTableIdentifier: "analytics.customers",
		},
	}

	t.Run("accepts existing table with new settings", func(t *testing.T) {
		require.NoError(t, ValidateResyncTableMappings(current, []*protos.TableMapping{{
			SourceTableIdentifier:      "public.orders",
			DestinationTableIdentifier: "analytics.orders",
			PartitionKey:               "toYYYYMM(created_at)",
		}}))
	})

	t.Run("requires destination identity", func(t *testing.T) {
		err := ValidateResyncTableMappings(current, []*protos.TableMapping{{
			SourceTableIdentifier: "public.customers",
		}})
		require.EqualError(t, err, "destination for table public.customers does not match the mirror configuration")
	})

	t.Run("rejects table outside mirror", func(t *testing.T) {
		err := ValidateResyncTableMappings(current, []*protos.TableMapping{{
			SourceTableIdentifier: "public.missing",
		}})
		require.EqualError(t, err, "table public.missing is not part of the mirror")
	})

	t.Run("rejects destination mismatch", func(t *testing.T) {
		err := ValidateResyncTableMappings(current, []*protos.TableMapping{{
			SourceTableIdentifier:      "public.orders",
			DestinationTableIdentifier: "analytics.other",
		}})
		require.EqualError(t, err, "destination for table public.orders does not match the mirror configuration")
	})

	t.Run("rejects duplicates", func(t *testing.T) {
		err := ValidateResyncTableMappings(current, []*protos.TableMapping{
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
