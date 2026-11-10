package model

import (
	"slices"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// ApplyTableSchemaDelta updates schema in place to match delta: renames first, then type changes,
// drops and adds, the order the source emits them in. It returns the names of added columns it skipped
// because the schema already had them.
func ApplyTableSchemaDelta(schema *protos.TableSchema, delta *protos.TableSchemaDelta) []string {
	for _, rename := range delta.GetRenamedColumns() {
		for _, col := range schema.Columns {
			if col.Name == rename.OldName {
				col.Name = rename.NewName
				col.PreviousNames = append([]string{rename.OldName}, col.PreviousNames...)
				break
			}
		}
		for i, key := range schema.PrimaryKeyColumns {
			if key == rename.OldName {
				schema.PrimaryKeyColumns[i] = rename.NewName
			}
		}
	}
	for _, changed := range delta.GetChangedColumns() {
		for _, col := range schema.Columns {
			if col.Name == changed.Name {
				col.Type = changed.Type
				col.TypeModifier = changed.TypeModifier
				col.TypeSchemaName = changed.TypeSchemaName
				col.Nullable = changed.Nullable
				break
			}
		}
	}
	if dropped := delta.GetDroppedColumns(); len(dropped) > 0 {
		schema.Columns = slices.DeleteFunc(schema.Columns, func(col *protos.FieldDescription) bool {
			return slices.Contains(dropped, col.Name)
		})
	}
	var skipped []string
	for _, added := range delta.GetAddedColumns() {
		if slices.ContainsFunc(schema.Columns, func(col *protos.FieldDescription) bool { return col.Name == added.Name }) {
			skipped = append(skipped, added.Name)
			continue
		}
		schema.Columns = append(schema.Columns, added)
	}
	return skipped
}

// HasDestructiveChanges reports whether delta drops, renames or retypes columns, which only
// destinations that replay those changes may apply to their catalog schema.
func HasDestructiveChanges(delta *protos.TableSchemaDelta) bool {
	return len(delta.GetDroppedColumns()) > 0 || len(delta.GetRenamedColumns()) > 0 || len(delta.GetChangedColumns()) > 0
}

// WithoutDestructiveChanges returns delta keeping only its added columns.
func WithoutDestructiveChanges(delta *protos.TableSchemaDelta) *protos.TableSchemaDelta {
	if !HasDestructiveChanges(delta) {
		return delta
	}
	return &protos.TableSchemaDelta{
		SrcTableName:    delta.SrcTableName,
		DstTableName:    delta.DstTableName,
		AddedColumns:    delta.AddedColumns,
		System:          delta.System,
		NullableEnabled: delta.NullableEnabled,
	}
}
