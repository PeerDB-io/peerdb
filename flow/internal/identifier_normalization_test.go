package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

func qt(namespace string, table string) *protos.QualifiedTable {
	return &protos.QualifiedTable{Namespace: namespace, Table: table}
}

func assertQT(t *testing.T, expected *protos.QualifiedTable, actual *protos.QualifiedTable) {
	t.Helper()
	require.NotNil(t, actual)
	assert.Equal(t, expected.Namespace, actual.Namespace)
	assert.Equal(t, expected.Table, actual.Table)
}

func TestNormalizeTableMappingLegacyOnly(t *testing.T) {
	tm := &protos.TableMapping{
		SourceTableIdentifier:      "src_schema.src_table",
		DestinationTableIdentifier: "dst_table",
	}
	NormalizeTableMapping(tm)
	assertQT(t, qt("src_schema", "src_table"), tm.SourceTable)
	assertQT(t, qt("", "dst_table"), tm.DestinationTable)
	assert.Empty(t, tm.SourceTableIdentifier)
	assert.Empty(t, tm.DestinationTableIdentifier)
}

func TestNormalizeTableMappingStructWins(t *testing.T) {
	tm := &protos.TableMapping{
		SourceTableIdentifier: "ignored.legacy",
		SourceTable:           qt("sch.ema", "ta.ble"),
		DestinationTable:      qt("", "dst.table"),
	}
	NormalizeTableMapping(tm)
	assertQT(t, qt("sch.ema", "ta.ble"), tm.SourceTable)
	assertQT(t, qt("", "dst.table"), tm.DestinationTable)
	assert.Empty(t, tm.SourceTableIdentifier)
}

func TestNormalizeTableMappingIdempotent(t *testing.T) {
	tm := &protos.TableMapping{SourceTableIdentifier: "a.b", DestinationTableIdentifier: "c.d"}
	NormalizeTableMapping(tm)
	NormalizeTableMapping(tm)
	assertQT(t, qt("a", "b"), tm.SourceTable)
	assertQT(t, qt("c", "d"), tm.DestinationTable)
}

func TestDenormalizeTableMappings(t *testing.T) {
	mappings := []*protos.TableMapping{{
		SourceTable:      qt("a", "b"),
		DestinationTable: qt("", "d"),
	}}
	DenormalizeTableMappings(mappings)
	assert.Equal(t, "a.b", mappings[0].SourceTableIdentifier)
	assert.Equal(t, "d", mappings[0].DestinationTableIdentifier)
	assertQT(t, qt("a", "b"), mappings[0].SourceTable)
}

func TestNormalizeQRepConfig(t *testing.T) {
	cfg := &protos.QRepConfig{
		WatermarkTable:             "public.events",
		DestinationTableIdentifier: "tgt.events",
	}
	NormalizeQRepConfig(cfg)
	assertQT(t, qt("public", "events"), cfg.QualifiedWatermarkTable)
	assertQT(t, qt("tgt", "events"), cfg.DestinationTable)
	assert.Empty(t, cfg.WatermarkTable)
	assert.Empty(t, cfg.DestinationTableIdentifier)

	DenormalizeQRepConfig(cfg)
	assert.Equal(t, "public.events", cfg.WatermarkTable)
	assert.Equal(t, "tgt.events", cfg.DestinationTableIdentifier)
}

func TestNormalizeSyncFlowOptions(t *testing.T) {
	opts := &protos.SyncFlowOptions{
		SrcTableIdNameMapping: map[uint32]string{1: "public.t1", 2: "t2"},
		TableMappings: []*protos.TableMapping{
			{SourceTableIdentifier: "public.t1", DestinationTableIdentifier: "t1"},
		},
	}
	NormalizeSyncFlowOptions(opts)
	require.Len(t, opts.SrcTableIdMapping, 2)
	assertQT(t, qt("public", "t1"), opts.SrcTableIdMapping[1])
	assertQT(t, qt("", "t2"), opts.SrcTableIdMapping[2])
	assert.Nil(t, opts.SrcTableIdNameMapping)
	assertQT(t, qt("public", "t1"), opts.TableMappings[0].SourceTable)

	NormalizeSyncFlowOptions(opts)
	require.Len(t, opts.SrcTableIdMapping, 2)
}

func TestNormalizeSetupReplicationInput(t *testing.T) {
	input := &protos.SetupReplicationInput{
		TableNameMapping: map[string]string{
			"public.b": "dst_b",
			"public.a": "dst_a",
		},
	}
	NormalizeSetupReplicationInput(input)
	require.Len(t, input.QualifiedTableMappings, 2)
	// deterministic order regardless of map iteration
	assertQT(t, qt("public", "a"), input.QualifiedTableMappings[0].Source)
	assertQT(t, qt("", "dst_a"), input.QualifiedTableMappings[0].Destination)
	assertQT(t, qt("public", "b"), input.QualifiedTableMappings[1].Source)
	assert.Nil(t, input.TableNameMapping)
}

func TestNormalizeEnsurePullabilityInput(t *testing.T) {
	input := &protos.EnsurePullabilityBatchInput{
		SourceTableIdentifiers: []string{"public.a", "b"},
	}
	NormalizeEnsurePullabilityInput(input)
	require.Len(t, input.SourceTables, 2)
	assertQT(t, qt("public", "a"), input.SourceTables[0])
	assertQT(t, qt("", "b"), input.SourceTables[1])
	assert.Nil(t, input.SourceTableIdentifiers)
}

func TestNormalizeRenameTablesInput(t *testing.T) {
	input := &protos.RenameTablesInput{
		RenameTableOptions: []*protos.RenameTableOption{
			{CurrentName: "public.t1_resync", NewName: "public.t1"},
		},
	}
	NormalizeRenameTablesInput(input)
	assertQT(t, qt("public", "t1_resync"), input.RenameTableOptions[0].CurrentTable)
	assertQT(t, qt("public", "t1"), input.RenameTableOptions[0].NewTable)
	assert.Empty(t, input.RenameTableOptions[0].CurrentName)
}

func TestNormalizeQRepPartition(t *testing.T) {
	partition := &protos.QRepPartition{
		ChildTableRanges: []*protos.ChildTableRange{
			{Table: "public.part_1", Start: 0, End: 10},
		},
	}
	NormalizeQRepPartition(partition)
	assertQT(t, qt("public", "part_1"), partition.ChildTableRanges[0].ChildTable)
	assert.Empty(t, partition.ChildTableRanges[0].Table)
}

func TestNormalizeTableSchema(t *testing.T) {
	ts := &protos.TableSchema{TableIdentifier: "public.t1"}
	NormalizeTableSchema(ts)
	assertQT(t, qt("public", "t1"), ts.Table)
	assert.Empty(t, ts.TableIdentifier)

	DenormalizeTableSchema(ts)
	assert.Equal(t, "public.t1", ts.TableIdentifier)
}

func TestNormalizeRemoveTablesFromRawTableInput(t *testing.T) {
	input := &protos.RemoveTablesFromRawTableInput{
		DestinationTableNames: []string{"a.b", "c"},
	}
	NormalizeRemoveTablesFromRawTableInput(input)
	require.Len(t, input.DestinationTables, 2)
	assertQT(t, qt("a", "b"), input.DestinationTables[0])
	assertQT(t, qt("", "c"), input.DestinationTables[1])
	assert.Nil(t, input.DestinationTableNames)
}

// pins the wire format: bytes serialized by a pre-QualifiedTable release (legacy string
// fields only) must normalize correctly after deserialization
func TestNormalizeOldWireFormat(t *testing.T) {
	oldBytes, err := proto.Marshal(&protos.FlowConnectionConfigsCore{
		FlowJobName: "old_flow",
		TableMappings: []*protos.TableMapping{
			{SourceTableIdentifier: "public.t1", DestinationTableIdentifier: "public.t1_dst"},
		},
	})
	require.NoError(t, err)

	cfg := &protos.FlowConnectionConfigsCore{}
	require.NoError(t, proto.Unmarshal(oldBytes, cfg))
	NormalizeFlowConfig(cfg)
	assertQT(t, qt("public", "t1"), cfg.TableMappings[0].SourceTable)
	assertQT(t, qt("public", "t1_dst"), cfg.TableMappings[0].DestinationTable)
}

func TestQualifiedTableProtoRoundTrip(t *testing.T) {
	table := common.QualifiedTable{Namespace: "sch.ema", Table: "ta.ble"}
	assert.Equal(t, table, QualifiedTableFromProto(QualifiedTableProto(table)))
	assert.Equal(t, common.QualifiedTable{}, QualifiedTableFromProto(nil))
}

// Continue-As-New payloads are denormalized so a rolled-back pre-QualifiedTable
// release can read them; round-tripping back through normalization must be lossless.
func TestDenormalizeSyncFlowOptionsRoundTrip(t *testing.T) {
	opts := &protos.SyncFlowOptions{
		SrcTableIdMapping: map[uint32]*protos.QualifiedTable{
			1: {Namespace: "public", Table: "t1"},
			2: {Table: "t2"},
		},
		TableMappings: []*protos.TableMapping{
			{
				SourceTable:      &protos.QualifiedTable{Namespace: "public", Table: "t1"},
				DestinationTable: &protos.QualifiedTable{Table: "t1_dst"},
			},
		},
	}
	DenormalizeSyncFlowOptions(opts)
	assert.Equal(t, map[uint32]string{1: "public.t1", 2: "t2"}, opts.SrcTableIdNameMapping)
	assert.Equal(t, "public.t1", opts.TableMappings[0].SourceTableIdentifier)
	assert.Equal(t, "t1_dst", opts.TableMappings[0].DestinationTableIdentifier)

	NormalizeSyncFlowOptions(opts)
	assertQT(t, qt("public", "t1"), opts.SrcTableIdMapping[1])
	assertQT(t, qt("", "t2"), opts.SrcTableIdMapping[2])
	assertQT(t, qt("public", "t1"), opts.TableMappings[0].SourceTable)
	assert.Nil(t, opts.SrcTableIdNameMapping)
	assert.Empty(t, opts.TableMappings[0].SourceTableIdentifier)
}

func TestDenormalizeDropFlowInput(t *testing.T) {
	input := &protos.DropFlowInput{
		FlowConnectionConfigs: &protos.FlowConnectionConfigsCore{
			TableMappings: []*protos.TableMapping{
				{
					SourceTable:      &protos.QualifiedTable{Namespace: "sch.ema", Table: "ta.ble"},
					DestinationTable: &protos.QualifiedTable{Table: "dst.table"},
				},
			},
		},
	}
	DenormalizeDropFlowInput(input)
	assert.Equal(t, "sch.ema.ta.ble", input.FlowConnectionConfigs.TableMappings[0].SourceTableIdentifier)
	assert.Equal(t, "dst.table", input.FlowConnectionConfigs.TableMappings[0].DestinationTableIdentifier)
	DenormalizeDropFlowInput(nil)
}

func TestNormalizeDropFlowInput(t *testing.T) {
	input := &protos.DropFlowInput{
		FlowConnectionConfigs: &protos.FlowConnectionConfigsCore{
			TableMappings: []*protos.TableMapping{
				{SourceTableIdentifier: "sch.ema.ta.ble", DestinationTableIdentifier: "dst.table"},
			},
		},
	}
	NormalizeDropFlowInput(input)
	tm := input.FlowConnectionConfigs.TableMappings[0]
	assertQT(t, qt("sch", "ema.ta.ble"), tm.SourceTable)
	assertQT(t, qt("dst", "table"), tm.DestinationTable)
	assert.Empty(t, tm.SourceTableIdentifier)
	assert.Empty(t, tm.DestinationTableIdentifier)
	NormalizeDropFlowInput(nil)
}

func TestNormalizeCreateRawTableInput(t *testing.T) {
	input := &protos.CreateRawTableInput{
		TableNameMapping: map[string]string{
			"public.t1": "t1_dst",
			"public.t2": "sch.t2_dst",
		},
	}
	NormalizeCreateRawTableInput(input)
	require.Len(t, input.QualifiedTableMappings, 2)
	assert.Nil(t, input.TableNameMapping)
	// deterministic ordering: sorted by (namespace, table)
	assertQT(t, qt("public", "t1"), input.QualifiedTableMappings[0].Source)
	assertQT(t, qt("", "t1_dst"), input.QualifiedTableMappings[0].Destination)
	assertQT(t, qt("public", "t2"), input.QualifiedTableMappings[1].Source)
	assertQT(t, qt("sch", "t2_dst"), input.QualifiedTableMappings[1].Destination)

	// idempotent: struct mappings win, nothing re-derived
	NormalizeCreateRawTableInput(input)
	require.Len(t, input.QualifiedTableMappings, 2)
	NormalizeCreateRawTableInput(nil)
}

func TestNormalizeTableSchemaDeltas(t *testing.T) {
	deltas := []*protos.TableSchemaDelta{
		{SrcTableName: "public.src", DstTableName: "dst.table"},
		{SrcTable: qt("sch", "already"), DstTable: qt("", "already_dst")},
	}
	NormalizeTableSchemaDeltas(deltas)
	assertQT(t, qt("public", "src"), deltas[0].SrcTable)
	assertQT(t, qt("dst", "table"), deltas[0].DstTable)
	assert.Empty(t, deltas[0].SrcTableName)
	assert.Empty(t, deltas[0].DstTableName)
	assertQT(t, qt("sch", "already"), deltas[1].SrcTable)
	NormalizeTableSchemaDelta(nil)
}

func TestNormalizeAndDenormalizeFlowConfigAPIRoundTrip(t *testing.T) {
	cfg := &protos.FlowConnectionConfigs{ //nolint:gocritic // NormalizeFlowConfigAPI operates on the API message
		TableMappings: []*protos.TableMapping{
			{SourceTableIdentifier: "sch.ema.ta.ble", DestinationTableIdentifier: "dst.table"},
		},
	}
	NormalizeFlowConfigAPI(cfg)
	tm := cfg.TableMappings[0]
	assertQT(t, qt("sch", "ema.ta.ble"), tm.SourceTable)
	assertQT(t, qt("dst", "table"), tm.DestinationTable)
	assert.Empty(t, tm.SourceTableIdentifier)

	DenormalizeFlowConfigForAPI(cfg)
	assert.Equal(t, "sch.ema.ta.ble", tm.SourceTableIdentifier)
	assert.Equal(t, "dst.table", tm.DestinationTableIdentifier)
	assertQT(t, qt("sch", "ema.ta.ble"), tm.SourceTable)

	// denormalize→normalize round-trips losslessly (struct wins, strings cleared)
	NormalizeFlowConfigAPI(cfg)
	assertQT(t, qt("sch", "ema.ta.ble"), tm.SourceTable)
	assert.Empty(t, tm.SourceTableIdentifier)
	NormalizeFlowConfigAPI(nil)
	DenormalizeFlowConfigForAPI(nil)
}
