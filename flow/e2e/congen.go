package e2e

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
)

type SuiteSource interface {
	Teardown(t *testing.T, ctx context.Context, suffix string)
	GeneratePeer(t *testing.T) *protos.Peer
	Connector() connectors.Connector
	Exec(ctx context.Context, sql string) error
	GetRows(ctx context.Context, suffix, table, cols string) (*model.QRecordBatch, error)
}

func TableMappings(s GenericSuite, tables ...string) []*protos.TableMapping {
	if len(tables)&1 != 0 {
		panic("must receive even number of table names")
	}
	tm := make([]*protos.TableMapping, 0, len(tables)/2)
	for i := 0; i < len(tables); i += 2 {
		tm = append(tm, &protos.TableMapping{
			SourceTableIdentifier:      AttachSchema(s, tables[i]),
			DestinationTableIdentifier: s.DestinationTable(tables[i+1]),
		})
	}
	return tm
}

func CreatePeer(t *testing.T, peer *protos.Peer) {
	t.Helper()
	pool, err := internal.GetCatalogConnectionPoolFromEnv(t.Context())
	require.NoError(t, err)
	res, err := utils.CreatePeerNoValidate(t.Context(), pool, peer, false)
	require.NoError(t, err)
	if res.Status != protos.CreatePeerStatus_CREATED {
		require.Fail(t, res.Message)
	}
}

type FlowConnectionGenerationConfig struct {
	FlowJobName      string
	TableNameMapping map[string]string
	Destination      string
	TableMappings    []*protos.TableMapping
	SoftDelete       bool
}

func (c *FlowConnectionGenerationConfig) GenerateFlowConnectionConfigs(s Suite) *protos.FlowConnectionConfigs {
	t := s.T()
	t.Helper()
	tblMappings := c.TableMappings
	if tblMappings == nil {
		for k, v := range c.TableNameMapping {
			tblMappings = append(tblMappings, &protos.TableMapping{
				SourceTableIdentifier:      k,
				DestinationTableIdentifier: v,
			})
		}
	}

	ret := &protos.FlowConnectionConfigs{
		FlowJobName:        c.FlowJobName,
		TableMappings:      tblMappings,
		SourceName:         s.Source().GeneratePeer(t).Name,
		DestinationName:    c.Destination,
		SyncedAtColName:    "_PEERDB_SYNCED_AT",
		IdleTimeoutSeconds: 15,
	}
	if c.SoftDelete {
		ret.SoftDeleteColName = "_PEERDB_IS_DELETED"
	}
	return ret
}
