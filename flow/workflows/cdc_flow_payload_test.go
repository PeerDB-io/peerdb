package peerflow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/workflows/cdc_state"
)

func TestContinueAsNewPayloadOmitsDuplicateTableMappings(t *testing.T) {
	const numTables = 8464
	const temporalPayloadLimit = 2 * 1024 * 1024

	tableMappings := make([]*protos.TableMapping, 0, numTables)
	srcTableIdNameMapping := make(map[uint32]string, numTables)
	for i := range numTables {
		source := fmt.Sprintf("schema_%02d.table_name_%04d", i%100, i)
		tableMappings = append(tableMappings, &protos.TableMapping{
			SourceTableIdentifier:      source,
			DestinationTableIdentifier: fmt.Sprintf("schema_%02d.table_name_%04d", i%100, i),
			PartitionKey:               "id",
			Exclude:                    []string{"internal_notes"},
		})
		srcTableIdNameMapping[uint32(i)] = source
	}
	cfg := &protos.FlowConnectionConfigsCore{
		FlowJobName:     "mirror",
		SourceName:      "source",
		DestinationName: "destination",
		TableMappings:   tableMappings,
	}
	state := cdc_state.CDCFlowWorkflowState{SyncFlowOptions: &protos.SyncFlowOptions{
		SrcTableIdNameMapping: srcTableIdNameMapping,
		TableMappings:         tableMappings,
	}}

	payloadSize := func(args ...any) int {
		payloads, err := converter.GetDefaultDataConverter().ToPayloads(args...)
		require.NoError(t, err)
		return proto.Size(payloads)
	}
	fullSize := payloadSize(cfg, &state)
	compactCfg := withoutTableMappings(cfg)
	compactSize := payloadSize(compactCfg, &state)
	t.Logf("Continue-As-New payload: full=%d compact=%d", fullSize, compactSize)
	require.Greater(t, fullSize, temporalPayloadLimit)
	require.Less(t, compactSize, temporalPayloadLimit)
	require.Less(t, payloadSize(compactCfg, state.SyncFlowOptions), temporalPayloadLimit)
	require.Len(t, cfg.TableMappings, numTables, "withoutTableMappings must not modify its input")

	payloads, err := converter.GetDefaultDataConverter().ToPayloads(compactCfg, &state)
	require.NoError(t, err)
	var nextCfg *protos.FlowConnectionConfigsCore
	var nextState *cdc_state.CDCFlowWorkflowState
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(payloads, &nextCfg, &nextState))
	require.Empty(t, nextCfg.TableMappings)
	restoreTableMappings(nextCfg, nextState)
	require.Len(t, nextCfg.TableMappings, numTables)
	for i, tableMapping := range nextCfg.TableMappings {
		require.True(t, proto.Equal(tableMappings[i], tableMapping))
		require.NotSame(t, nextState.SyncFlowOptions.TableMappings[i], tableMapping)
	}
}

func TestRestoreTableMappingsKeepsLegacyInput(t *testing.T) {
	cfgMapping := &protos.TableMapping{SourceTableIdentifier: "public.a", DestinationTableIdentifier: "a"}
	cfg := &protos.FlowConnectionConfigsCore{TableMappings: []*protos.TableMapping{cfgMapping}}
	state := &cdc_state.CDCFlowWorkflowState{SyncFlowOptions: &protos.SyncFlowOptions{
		TableMappings: []*protos.TableMapping{{SourceTableIdentifier: "public.a", DestinationTableIdentifier: "a"}},
	}}
	restoreTableMappings(cfg, state)
	require.Len(t, cfg.TableMappings, 1)
	require.Same(t, cfgMapping, cfg.TableMappings[0])
}
