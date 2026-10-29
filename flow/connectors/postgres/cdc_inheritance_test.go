package connpostgres

import (
	"log/slog"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	pkg_pg "github.com/PeerDB-io/peerdb/flow/pkg/postgres"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

const (
	inhParentRelID uint32 = 100
	inhChildARelID uint32 = 101
	inhChildBRelID uint32 = 102
	inhChildCRelID uint32 = 103
)

// newInheritanceTestCDCSource builds a CDC source mirroring public.parent, whose inheritance children
// 101 and 102 are already known from the startup map.
func newInheritanceTestCDCSource(t *testing.T) *PostgresCDCSource {
	t.Helper()
	return &PostgresCDCSource{
		PostgresConnector: &PostgresConnector{
			logger:            log.NewStructuredLogger(slog.Default()),
			typeMap:           pgtype.NewMap(),
			customTypeMapping: map[uint32]pkg_pg.CustomDataType{},
			hushWarnOID:       map[uint32]struct{}{},
		},
		otelManager:           &otel_metrics.OtelManager{},
		srcTableIDNameMapping: map[uint32]string{inhParentRelID: "public.parent"},
		tableNameMapping: map[string]model.SourceTableMapping{
			"public.parent": {Name: "parent", Exclude: map[string]struct{}{}},
		},
		tableNameSchemaMapping: map[string]*protos.TableSchema{
			"parent": {
				TableIdentifier: "public.parent",
				System:          protos.TypeSystem_Q,
				Columns: []*protos.FieldDescription{
					{Name: "id", Type: string(types.QValueKindInt64)},
					{Name: "name", Type: string(types.QValueKindString)},
					{Name: "val", Type: string(types.QValueKindString)},
					{Name: "n", Type: string(types.QValueKindInt32)},
				},
			},
		},
		relationMessageMapping:       model.RelationMessageMapping{},
		childToParentRelIDMapping:    map[uint32]uint32{inhChildARelID: inhParentRelID, inhChildBRelID: inhParentRelID},
		idToRelKindMap:               map[uint32]byte{inhParentRelID: 'r'},
		mirroredRelIDs:               []uint32{inhParentRelID},
		hushWarnUnknownTableDetected: map[uint32]struct{}{},
		emittedAddedColumns:          map[string]addedColumnType{},
		warnedColumnEvents:           map[string]struct{}{},
	}
}

func relColumn(name string, oid uint32) *pglogrepl.RelationMessageColumn {
	return &pglogrepl.RelationMessageColumn{Name: name, DataType: oid, TypeModifier: -1}
}

// child A has the parent's column order, child B was created standalone and then INHERITed,
// so its text columns are swapped
func childARelation() *pglogrepl.RelationMessage {
	return &pglogrepl.RelationMessage{
		RelationID: inhChildARelID, Namespace: "tenant_a", RelationName: "parent",
		Columns: []*pglogrepl.RelationMessageColumn{
			relColumn("id", pgtype.Int8OID), relColumn("name", pgtype.TextOID),
			relColumn("val", pgtype.TextOID), relColumn("n", pgtype.Int4OID),
		},
		ColumnNum: 4,
	}
}

func childBRelation() *pglogrepl.RelationMessage {
	return &pglogrepl.RelationMessage{
		RelationID: inhChildBRelID, Namespace: "tenant_b", RelationName: "parent",
		Columns: []*pglogrepl.RelationMessageColumn{
			relColumn("id", pgtype.Int8OID), relColumn("val", pgtype.TextOID),
			relColumn("name", pgtype.TextOID), relColumn("n", pgtype.Int4OID),
		},
		ColumnNum: 4,
	}
}

// tuple builds a text-format tuple; nil entries are SQL NULLs
func tuple(values ...*string) *pglogrepl.TupleData {
	columns := make([]*pglogrepl.TupleDataColumn, 0, len(values))
	for _, v := range values {
		if v == nil {
			columns = append(columns, &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeNull})
		} else {
			columns = append(columns, &pglogrepl.TupleDataColumn{
				DataType: pglogrepl.TupleDataTypeText, Length: uint32(len(*v)), Data: []byte(*v),
			})
		}
	}
	return &pglogrepl.TupleData{ColumnNum: uint16(len(columns)), Columns: columns}
}

func str(v string) *string { return &v }

func requireRow(t *testing.T, items model.RecordItems, id int64, name string, val string, n int32) {
	t.Helper()
	require.Equal(t, types.QValueInt64{Val: id}, items.GetColumnValue("id"))
	require.Equal(t, types.QValueString{Val: name}, items.GetColumnValue("name"))
	require.Equal(t, types.QValueString{Val: val}, items.GetColumnValue("val"))
	require.Equal(t, types.QValueInt32{Val: n}, items.GetColumnValue("n"))
}

func TestInheritanceChildrenDecodeWithOwnLayout(t *testing.T) {
	t.Parallel()
	p := newInheritanceTestCDCSource(t)
	ctx := t.Context()
	customTypes := p.customTypeMapping

	// pgoutput announces each child before its first tuple: A first, then B
	relA, relB := childARelation(), childBRelation()
	rec, err := processRelationMessage[model.RecordItems](ctx, p, 1, relA, inhParentRelID)
	require.NoError(t, err)
	require.Nil(t, rec, "child A has no columns beyond the destination schema")
	rec, err = processRelationMessage[model.RecordItems](ctx, p, 2, relB, inhParentRelID)
	require.NoError(t, err)
	require.Nil(t, rec)

	require.Same(t, relA, p.relationMessageMapping[inhChildARelID])
	require.Same(t, relB, p.relationMessageMapping[inhChildBRelID])
	require.NotContains(t, p.relationMessageMapping, inhParentRelID, "children must not overwrite a parent entry")

	// A's tuple arrives after B's RelationMessage was the last one seen
	insertA, err := processInsertMessage(p, 3, &pglogrepl.InsertMessage{
		RelationID: inhChildARelID, Tuple: tuple(str("1"), str("alice"), str("a-val"), str("7")),
	}, qProcessor{}, customTypes)
	require.NoError(t, err)
	ins := insertA.(*model.InsertRecord[model.RecordItems])
	require.Equal(t, "parent", ins.DestinationTableName)
	require.Equal(t, "public.parent", ins.SourceTableName)
	requireRow(t, ins.Items, 1, "alice", "a-val", 7)

	insertB, err := processInsertMessage(p, 4, &pglogrepl.InsertMessage{
		RelationID: inhChildBRelID, Tuple: tuple(str("2"), str("b-val"), str("bob"), str("8")),
	}, qProcessor{}, customTypes)
	require.NoError(t, err)
	requireRow(t, insertB.(*model.InsertRecord[model.RecordItems]).Items, 2, "bob", "b-val", 8)

	// default replica identity: the old tuple only carries the key, other columns are NULL
	updateA, err := processUpdateMessage(p, 5, &pglogrepl.UpdateMessage{
		RelationID:   inhChildARelID,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeKey,
		OldTuple:     tuple(str("1"), nil, nil, nil),
		NewTuple:     tuple(str("1"), str("alice2"), str("a-val2"), str("9")),
	}, qProcessor{}, customTypes, map[string]struct{}{})
	require.NoError(t, err)
	upd := updateA.(*model.UpdateRecord[model.RecordItems])
	requireRow(t, upd.NewItems, 1, "alice2", "a-val2", 9)
	require.Equal(t, types.QValueInt64{Val: 1}, upd.OldItems.GetColumnValue("id"))

	// replica identity full on B: the old tuple is in B's layout too
	updateB, err := processUpdateMessage(p, 6, &pglogrepl.UpdateMessage{
		RelationID:   inhChildBRelID,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
		OldTuple:     tuple(str("2"), str("b-val"), str("bob"), str("8")),
		NewTuple:     tuple(str("2"), str("b-val2"), str("bob2"), str("10")),
	}, qProcessor{}, customTypes, map[string]struct{}{})
	require.NoError(t, err)
	updB := updateB.(*model.UpdateRecord[model.RecordItems])
	requireRow(t, updB.NewItems, 2, "bob2", "b-val2", 10)
	require.Equal(t, types.QValueString{Val: "bob"}, updB.OldItems.GetColumnValue("name"))

	deleteB, err := processDeleteMessage(p, 7, &pglogrepl.DeleteMessage{
		RelationID:   inhChildBRelID,
		OldTupleType: pglogrepl.DeleteMessageTupleTypeOld,
		OldTuple:     tuple(str("2"), str("b-val2"), str("bob2"), str("10")),
	}, qProcessor{}, customTypes)
	require.NoError(t, err)
	requireRow(t, deleteB.(*model.DeleteRecord[model.RecordItems]).Items, 2, "bob2", "b-val2", 10)
}

func TestInheritanceChildWithoutRelationMessageIsRejected(t *testing.T) {
	t.Parallel()
	p := newInheritanceTestCDCSource(t)
	_, err := processRelationMessage[model.RecordItems](t.Context(), p, 1, childARelation(), inhParentRelID)
	require.NoError(t, err)

	// B is a known child whose RelationMessage was never seen: decoding it with A's layout is never an option
	_, err = processInsertMessage(p, 2, &pglogrepl.InsertMessage{
		RelationID: inhChildBRelID, Tuple: tuple(str("2"), str("b-val"), str("bob"), str("8")),
	}, qProcessor{}, p.customTypeMapping)
	require.ErrorContains(t, err, "unknown relation id 102 (mapped to 100)")
}

func TestProcessTupleRejectsColumnCountMismatch(t *testing.T) {
	t.Parallel()
	p := newInheritanceTestCDCSource(t)
	rel := childARelation()
	mapping := p.tableNameMapping["public.parent"]

	_, _, err := processTuple(qProcessor{}, p, tuple(str("1"), str("x"), str("y")), rel, mapping, nil, "", model.BaseRecord{})
	require.ErrorContains(t, err, "column-count mismatch")
	require.ErrorContains(t, err, "3 != 4")

	longTuple := tuple(str("1"), str("x"), str("y"), str("2"), str("z"))
	_, _, err = processTuple(qProcessor{}, p, longTuple, rel, mapping, nil, "", model.BaseRecord{})
	require.ErrorContains(t, err, "5 != 4")

	items, _, err := processTuple(qProcessor{}, p, nil, rel, mapping, nil, "", model.BaseRecord{})
	require.NoError(t, err)
	require.Equal(t, 0, items.Len())
}

func TestChildOnlyColumnEmittedOncePerPull(t *testing.T) {
	t.Parallel()
	p := newInheritanceTestCDCSource(t)
	p.otelManager.Metrics.ColumnTypeChangesCounter = noop.Int64Counter{}
	// another child already emitted these columns in this pull (emitting reads the catalog, so the test starts after)
	p.emittedAddedColumns["parent\x00extra"] = addedColumnType{typ: string(types.QValueKindString), typmod: -1}
	p.emittedAddedColumns["parent\x00amount"] = addedColumnType{typ: string(types.QValueKindNumeric), typmod: 655366}
	withColumns := func(rel *pglogrepl.RelationMessage, columns ...*pglogrepl.RelationMessageColumn) *pglogrepl.RelationMessage {
		rel.Columns = append(rel.Columns, columns...)
		rel.ColumnNum = uint16(len(rel.Columns))
		return rel
	}

	// the same column with the same type is not emitted again
	rec, err := processRelationMessage[model.RecordItems](t.Context(), p, 1,
		withColumns(childBRelation(), relColumn("extra", pgtype.TextOID)), inhParentRelID)
	require.NoError(t, err)
	require.Nil(t, rec)
	conflicts := func() int {
		count := 0
		p.warnedTypeChanges.Range(func(_, _ any) bool { count++; return true })
		return count
	}
	require.Equal(t, 0, conflicts())

	// a different type, or the same type with a different precision and scale, is reported and not propagated
	amount := relColumn("amount", pgtype.NumericOID)
	amount.TypeModifier = 1310724
	rec, err = processRelationMessage[model.RecordItems](t.Context(), p, 2,
		withColumns(childARelation(), relColumn("extra", pgtype.Int8OID), amount), inhParentRelID)
	require.NoError(t, err)
	require.Nil(t, rec)
	require.Equal(t, 2, conflicts())
}
