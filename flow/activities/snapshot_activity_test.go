package activities

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func textColumns(names ...string) []*protos.FieldDescription {
	columns := make([]*protos.FieldDescription, 0, len(names))
	for _, name := range names {
		columns = append(columns, &protos.FieldDescription{Name: name, Type: "text", TypeModifier: -1, Nullable: true})
	}
	return columns
}

func columnNamesOf(columns []*protos.FieldDescription) []string {
	names := make([]string, 0, len(columns))
	for _, col := range columns {
		names = append(names, col.Name)
	}
	return names
}

func TestSnapshotSchemaDeltasSourcesSharingDestination(t *testing.T) {
	logger := log.NewStructuredLogger(slog.Default())
	tableMappings := []*protos.TableMapping{
		{SourceTableIdentifier: "public.a", DestinationTableIdentifier: "public.d", Exclude: []string{"secret"}},
		{SourceTableIdentifier: "public.b", DestinationTableIdentifier: "public.d"},
		{SourceTableIdentifier: "public.c", DestinationTableIdentifier: "public.e"},
	}
	srcSchemas := map[string]*protos.TableSchema{
		"public.a": {Columns: textColumns("id", "val", "secret", "a_only", "shared")},
		"public.b": {Columns: textColumns("id", "shared", "val", "b_only")},
		"public.c": {Columns: textColumns("id", "val")},
	}
	dstSchema := &protos.TableSchema{
		TableIdentifier: "public.d",
		Columns:         textColumns("id", "val"),
		System:          protos.TypeSystem_PG,
		NullableEnabled: true,
	}
	dstSchemas := map[string]*protos.TableSchema{
		"public.d": dstSchema,
		"public.e": {TableIdentifier: "public.e", Columns: textColumns("id", "val")},
	}
	srcSchemasBefore := make(map[string]*protos.TableSchema, len(srcSchemas))
	for name, schema := range srcSchemas {
		srcSchemasBefore[name] = proto.CloneOf(schema)
	}
	dstSchemaBefore := proto.CloneOf(dstSchema)

	schemaDeltas, reconciled := snapshotSchemaDeltas(tableMappings, srcSchemas, dstSchemas, logger)

	// each column the destination lacks is added once, by the first source that has it
	require.Len(t, schemaDeltas, 2)
	require.Equal(t, "public.a", schemaDeltas[0].SrcTableName)
	require.Equal(t, "public.d", schemaDeltas[0].DstTableName)
	require.Equal(t, []string{"a_only", "shared"}, columnNamesOf(schemaDeltas[0].AddedColumns))
	require.Equal(t, protos.TypeSystem_PG, schemaDeltas[0].System)
	require.True(t, schemaDeltas[0].NullableEnabled)
	require.Equal(t, "public.b", schemaDeltas[1].SrcTableName)
	require.Equal(t, "public.d", schemaDeltas[1].DstTableName)
	require.Equal(t, []string{"b_only"}, columnNamesOf(schemaDeltas[1].AddedColumns))

	// with the deltas, the shared destination schema has the columns of both sources, excluded ones aside
	dstColumns := columnNamesOf(dstSchema.Columns)
	for _, schemaDelta := range schemaDeltas {
		dstColumns = append(dstColumns, columnNamesOf(schemaDelta.AddedColumns)...)
	}
	require.Equal(t, []string{"id", "val", "a_only", "shared", "b_only"}, dstColumns)
	require.Equal(t, map[string]bool{"public.a": true, "public.b": true, "public.c": true}, reconciled)

	// inputs, including the destination schema both sources share, are left as they were
	require.True(t, proto.Equal(dstSchemaBefore, dstSchemas["public.d"]))
	for name, schema := range srcSchemas {
		require.True(t, proto.Equal(srcSchemasBefore[name], schema), name)
	}
}

func TestSnapshotSchemaDeltasRetry(t *testing.T) {
	logger := log.NewStructuredLogger(slog.Default())
	tableMappings := []*protos.TableMapping{
		{SourceTableIdentifier: "public.a", DestinationTableIdentifier: "public.d"},
		{SourceTableIdentifier: "public.b", DestinationTableIdentifier: "public.d"},
	}
	srcSchemas := map[string]*protos.TableSchema{
		"public.a": {Columns: textColumns("id", "val", "added")},
		"public.b": {Columns: textColumns("id", "val")},
	}
	catalogSchemas := map[string]*protos.TableSchema{
		"public.d": {TableIdentifier: "public.d", Columns: textColumns("id", "val")},
	}

	allReconciled := map[string]bool{"public.a": true, "public.b": true}
	schemaDeltas, reconciled := snapshotSchemaDeltas(tableMappings, srcSchemas, catalogSchemas, logger)
	require.Len(t, schemaDeltas, 1)
	require.Equal(t, []string{"added"}, columnNamesOf(schemaDeltas[0].AddedColumns))
	require.Equal(t, allReconciled, reconciled)

	// retry after the destination was altered but the catalog update failed: the same columns are added again
	retryDeltas, retryReconciled := snapshotSchemaDeltas(tableMappings, srcSchemas, catalogSchemas, logger)
	require.Len(t, retryDeltas, 1)
	require.True(t, proto.Equal(schemaDeltas[0], retryDeltas[0]))
	require.Equal(t, allReconciled, retryReconciled)

	// retry after the catalog was updated: nothing is left to add, and the same tables are reconciled
	catalogSchemas["public.d"] = &protos.TableSchema{TableIdentifier: "public.d", Columns: textColumns("id", "val", "added")}
	doneDeltas, doneReconciled := snapshotSchemaDeltas(tableMappings, srcSchemas, catalogSchemas, logger)
	require.Empty(t, doneDeltas)
	require.Equal(t, allReconciled, doneReconciled)
}

func TestSnapshotSchemaDeltasNotReconciled(t *testing.T) {
	logger := log.NewStructuredLogger(slog.Default())
	tableMappings := []*protos.TableMapping{
		// same source in two mappings, each could exclude different columns
		{SourceTableIdentifier: "public.dup", DestinationTableIdentifier: "public.dup_1", Exclude: []string{"val"}},
		{SourceTableIdentifier: "public.dup", DestinationTableIdentifier: "public.dup_2"},
		{SourceTableIdentifier: "public.no_catalog", DestinationTableIdentifier: "public.no_catalog_dst"},
		{SourceTableIdentifier: "public.ok", DestinationTableIdentifier: "public.ok_dst"},
	}
	srcSchemas := map[string]*protos.TableSchema{
		"public.dup":        {Columns: textColumns("id", "val", "added")},
		"public.no_catalog": {Columns: textColumns("id", "added")},
		"public.ok":         {Columns: textColumns("id", "added")},
	}
	dstSchemas := map[string]*protos.TableSchema{
		"public.dup_1":  {Columns: textColumns("id")},
		"public.dup_2":  {Columns: textColumns("id", "val")},
		"public.ok_dst": {Columns: textColumns("id")},
	}

	schemaDeltas, reconciled := snapshotSchemaDeltas(tableMappings, srcSchemas, dstSchemas, logger)
	require.Len(t, schemaDeltas, 1)
	require.Equal(t, "public.ok", schemaDeltas[0].SrcTableName)
	require.Equal(t, []string{"added"}, columnNamesOf(schemaDeltas[0].AddedColumns))
	require.Equal(t, map[string]bool{"public.ok": true}, reconciled)
}
