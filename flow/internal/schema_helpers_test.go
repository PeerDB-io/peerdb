package internal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestValidateResyncTables(t *testing.T) {
	current := []*protos.TableMapping{
		{SourceTableIdentifier: "public.orders", DestinationTableIdentifier: "analytics.orders"},
		{SourceTableIdentifier: "public.customers", DestinationTableIdentifier: "analytics.customers"},
	}
	orders := &protos.TableMapping{
		SourceTableIdentifier:      "public.orders",
		DestinationTableIdentifier: "analytics.orders",
		PartitionKey:               "toYYYYMM(created_at)",
	}

	for _, tc := range []struct {
		name   string
		update *protos.CDCFlowConfigUpdate
		err    string
	}{
		{
			name: "accepts existing table with new settings",
			update: &protos.CDCFlowConfigUpdate{
				ResyncTables:     []*protos.TableMapping{orders},
				AdditionalTables: []*protos.TableMapping{{SourceTableIdentifier: "public.new"}},
				RemovedTables:    []*protos.TableMapping{{SourceTableIdentifier: "public.customers"}},
			},
		},
		{
			name: "requires source table",
			update: &protos.CDCFlowConfigUpdate{
				ResyncTables: []*protos.TableMapping{{DestinationTableIdentifier: "analytics.orders"}},
			},
			err: "resync table must have a source table identifier",
		},
		{
			name: "requires destination identity",
			update: &protos.CDCFlowConfigUpdate{
				ResyncTables: []*protos.TableMapping{{SourceTableIdentifier: "public.customers"}},
			},
			err: "destination for table public.customers does not match the mirror configuration",
		},
		{
			name: "rejects destination mismatch",
			update: &protos.CDCFlowConfigUpdate{
				ResyncTables: []*protos.TableMapping{
					{SourceTableIdentifier: "public.orders", DestinationTableIdentifier: "analytics.other"},
				},
			},
			err: "destination for table public.orders does not match the mirror configuration",
		},
		{
			name: "rejects table outside mirror",
			update: &protos.CDCFlowConfigUpdate{
				ResyncTables: []*protos.TableMapping{{SourceTableIdentifier: "public.missing"}},
			},
			err: "table public.missing is not part of the mirror",
		},
		{
			name: "rejects duplicates",
			update: &protos.CDCFlowConfigUpdate{
				ResyncTables: []*protos.TableMapping{orders, orders},
			},
			err: "table public.orders was requested for resync more than once",
		},
		{
			name: "rejects table added and resynced",
			update: &protos.CDCFlowConfigUpdate{
				ResyncTables:     []*protos.TableMapping{orders},
				AdditionalTables: []*protos.TableMapping{{SourceTableIdentifier: "public.orders"}},
			},
			err: "table public.orders cannot be added and resynced in the same update",
		},
		{
			name: "rejects table removed and resynced",
			update: &protos.CDCFlowConfigUpdate{
				ResyncTables:  []*protos.TableMapping{orders},
				RemovedTables: []*protos.TableMapping{{SourceTableIdentifier: "public.orders"}},
			},
			err: "table public.orders cannot be removed and resynced in the same update",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateResyncTables(current, tc.update)
			if tc.err == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.err)
			}
		})
	}
}
