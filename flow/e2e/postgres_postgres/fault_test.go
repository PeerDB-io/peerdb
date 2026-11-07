//go:build tilt

package postgres_postgres

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal/testhooks"
)

// faultWorkload adapts this suite's Postgres source and Postgres destination,
// which share one database and schema.
func (s PeerFlowE2ETestSuitePG) faultWorkload() e2e.FaultWorkload {
	t := s.t
	t.Helper()
	ctx := t.Context()
	source, err := e2e.NewPgFaultSource(ctx, s.Conn(), e2e.Schema(s))
	require.NoError(t, err)
	t.Cleanup(source.Pool.Close)
	dest, err := e2e.NewPgFaultDestination(ctx, s.Conn(), e2e.Schema(s), s.Peer())
	require.NoError(t, err)
	return e2e.FaultWorkload{
		Suite:    s,
		Source:   source,
		Dest:     dest,
		Progress: e2e.PgDestinationCDCProgress(dest),
		Tables: func(id string) []e2e.FaultTable {
			var tables []e2e.FaultTable
			for _, suffix := range []string{"a", "b"} {
				name := fmt.Sprintf("fault_%s_%s", id, suffix)
				tables = append(tables, e2e.FaultTable{
					Source:      name,
					Dest:        name + "_dst",
					CatalogName: e2e.AttachSchema(s, name+"_dst"),
					ProgressKey: e2e.ProgressMirror,
				})
			}
			return tables
		},
		FlowConfig: func(flowName string, tables []e2e.FaultTable) *protos.FlowConnectionConfigs {
			mapping := map[string]string{}
			for _, table := range tables {
				mapping[e2e.AttachSchema(s, table.Source)] = e2e.AttachSchema(s, table.Dest)
			}
			gen := e2e.FlowConnectionGenerationConfig{
				FlowJobName:      flowName,
				TableNameMapping: mapping,
				Destination:      s.Peer().Name,
			}
			config := gen.GenerateFlowConnectionConfigs(s)
			config.MaxBatchSize = 1
			config.IdleTimeoutSeconds = 1
			return config
		},
		Kind:         testhooks.WorkloadPGToPG,
		BufferLimit:  e2e.FaultBuffer + 1,
		SchemaChange: true,
	}
}

func (s PeerFlowE2ETestSuitePG) Test_FaultKnown_NormalizeMissesTheFinalRequestAndLeavesDataPending() {
	e2e.RunKnownFailure(s.faultWorkload(), "missed_req")
}

func (s PeerFlowE2ETestSuitePG) Test_FaultKnown_CDCSyncKeepsWaitingAfterNormalizationHasCaughtUp() {
	e2e.RunKnownFailure(s.faultWorkload(), "missed_done")
}

func (s PeerFlowE2ETestSuitePG) Test_FaultKnown_PostgreSQLMarksABatchSyncedBeforeUpdatingItsTables() {
	e2e.RunKnownFailure(s.faultWorkload(), "pg_ddl")
}

func (s PeerFlowE2ETestSuitePG) Test_FaultKnown_PostgreSQLRestartsWithAnOldCatalogColumnList() {
	e2e.RunKnownFailure(s.faultWorkload(), "pg_catalog")
}

func TestFaultShard(t *testing.T) {
	e2e.RunFaultShards(t, testhooks.WorkloadPGToPG, SetupPostgresSuite, PeerFlowE2ETestSuitePG.faultWorkload)
}
