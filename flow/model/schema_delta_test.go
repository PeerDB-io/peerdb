package model

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestApplyTableSchemaDelta(t *testing.T) {
	schema := &protos.TableSchema{
		PrimaryKeyColumns: []string{"id"},
		Columns: []*protos.FieldDescription{
			{Name: "id", Type: "int64"}, {Name: "a", Type: "int32"}, {Name: "b", Type: "string"}, {Name: "c", Type: "int32"},
		},
	}
	skipped := ApplyTableSchemaDelta(schema, &protos.TableSchemaDelta{
		RenamedColumns: []*protos.RenamedColumn{{OldName: "a", NewName: "a2"}},
		ChangedColumns: []*protos.FieldDescription{{Name: "a2", Type: "int64"}},
		DroppedColumns: []string{"b"},
		AddedColumns:   []*protos.FieldDescription{{Name: "d", Type: "string"}, {Name: "c", Type: "int32"}},
	})
	require.Equal(t, []string{"c"}, skipped)
	require.Len(t, schema.Columns, 4)
	require.Equal(t, "a2", schema.Columns[1].Name)
	require.Equal(t, "int64", schema.Columns[1].Type)
	require.Equal(t, []string{"a"}, schema.Columns[1].PreviousNames)
	require.Equal(t, "c", schema.Columns[2].Name)
	require.Equal(t, "d", schema.Columns[3].Name)
}
