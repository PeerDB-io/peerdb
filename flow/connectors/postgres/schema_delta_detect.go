package connpostgres

import (
	"context"
	"fmt"
	"log/slog"
	"slices"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// renameCandidate is the one replicated column a relation message lost and the one it gained,
// at the same position once excluded columns are left out.
type renameCandidate struct {
	oldName string
	newName string
	// leftNeighbor is the column before it, "" when it is the first column
	leftNeighbor string
	// last is true when it is the last column, where an ADD lands after a DROP of the last column too
	last bool
}

// findRenameCandidate compares two relation messages' column lists. Relation messages list columns
// in attnum order and RENAME keeps the attnum, while ADD always appends.
func findRenameCandidate(prevNames []string, currNames []string, exclude map[string]struct{}) (renameCandidate, bool) {
	keep := func(names []string) []string {
		return slices.DeleteFunc(slices.Clone(names), func(name string) bool {
			_, excluded := exclude[name]
			return excluded
		})
	}
	prev, curr := keep(prevNames), keep(currNames)
	missingIdx, addedIdx := -1, -1
	for i, name := range prev {
		if !slices.Contains(curr, name) {
			if missingIdx != -1 {
				return renameCandidate{}, false
			}
			missingIdx = i
		}
	}
	for i, name := range curr {
		if !slices.Contains(prev, name) {
			if addedIdx != -1 {
				return renameCandidate{}, false
			}
			addedIdx = i
		}
	}
	if missingIdx == -1 || missingIdx != addedIdx {
		return renameCandidate{}, false
	}
	candidate := renameCandidate{oldName: prev[missingIdx], newName: curr[addedIdx], last: missingIdx == len(prev)-1}
	if missingIdx > 0 {
		candidate.leftNeighbor = prev[missingIdx-1]
	}
	return candidate, true
}

// confirmedBy checks the candidate against the source's current pg_attribute. A column before an existing one
// can only be a rename. For the last column, DROP + ADD leaves a dropped attribute between the left neighbor
// and the new column, a rename does not.
// shortcut: a column dropped long before, sitting between the left neighbor and a renamed last column, makes the
// rename look like DROP + ADD; tracking attnums in the catalog schema would remove the ambiguity.
func (c renameCandidate) confirmedBy(liveAttnums map[string]int16, droppedAttnums []int16) bool {
	newAttnum, ok := liveAttnums[c.newName]
	if !ok {
		return false
	}
	if !c.last {
		return true
	}
	var leftAttnum int16
	if c.leftNeighbor != "" {
		if leftAttnum, ok = liveAttnums[c.leftNeighbor]; !ok {
			return false
		}
	}
	return !slices.ContainsFunc(droppedAttnums, func(attnum int16) bool {
		return attnum > leftAttnum && attnum < newAttnum
	})
}

type relAttribute struct {
	attnum     int16
	notNull    bool
	hasDefault bool
	// a column added with a constant default has a missing value; one added with a volatile default
	// rewrote the table instead
	hasMissing bool
}

// fetchRelationAttributes returns a relation's live columns by name, and the attnums of its dropped columns.
func (c *PostgresConnector) fetchRelationAttributes(ctx context.Context, relID uint32) (map[string]relAttribute, []int16, error) {
	rows, err := c.conn.Query(ctx,
		"SELECT attname, attnum, attisdropped, attnotnull, atthasdef, atthasmissing FROM pg_attribute"+
			" WHERE attrelid = $1 AND attnum > 0", relID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query attributes of relation %d: %w", relID, err)
	}
	defer rows.Close()
	live := make(map[string]relAttribute)
	var dropped []int16
	for rows.Next() {
		var name string
		var attr relAttribute
		var isDropped bool
		if err := rows.Scan(&name, &attr.attnum, &isDropped, &attr.notNull, &attr.hasDefault, &attr.hasMissing); err != nil {
			return nil, nil, fmt.Errorf("failed to scan attributes of relation %d: %w", relID, err)
		}
		if isDropped {
			dropped = append(dropped, attr.attnum)
		} else {
			live[name] = attr
		}
	}
	return live, dropped, rows.Err()
}

// isWideningTypeChange reports whether ClickHouse converting the stored values gives what PostgreSQL's
// cast gave. Any other type change may have run a USING expression, so the table is re-snapshotted.
// shortcut: a widening change with a custom USING expression is still replayed as a plain conversion.
func isWideningTypeChange(from, to types.QValueKind, fromTypmod, toTypmod int32) bool {
	intRank := map[types.QValueKind]int{types.QValueKindInt16: 1, types.QValueKindInt32: 2, types.QValueKindInt64: 3}
	if fromRank, ok := intRank[from]; ok {
		if toRank, ok := intRank[to]; ok {
			return toRank >= fromRank
		}
		return to == types.QValueKindNumeric
	}
	switch {
	case from == types.QValueKindFloat32 && to == types.QValueKindFloat64:
		return true
	case from == types.QValueKindNumeric && to == types.QValueKindNumeric:
		if toTypmod < 0 {
			return true
		}
		if fromTypmod < 0 {
			return false
		}
		fromPrecision, fromScale := common.ParseNumericTypmod(fromTypmod)
		toPrecision, toScale := common.ParseNumericTypmod(toTypmod)
		return toScale >= fromScale && toPrecision-toScale >= fromPrecision-fromScale
	}
	return false
}

// resyncReason says why schemaDelta cannot be replayed in place, or "" when it can.
func (p *PostgresCDCSource) resyncReason(
	ctx context.Context,
	relID uint32,
	prevSchema *protos.TableSchema,
	schemaDelta *protos.TableSchemaDelta,
	attrs map[string]relAttribute,
) string {
	prevName := make(map[string]string, len(prevSchema.Columns))
	prevColumns := make(map[string]*protos.FieldDescription, len(prevSchema.Columns))
	for _, col := range prevSchema.Columns {
		prevName[col.Name] = col.Name
		prevColumns[col.Name] = col
	}
	for _, rename := range schemaDelta.RenamedColumns {
		prevName[rename.NewName] = rename.OldName
	}
	isKey := func(name string) bool { return slices.Contains(prevSchema.PrimaryKeyColumns, name) }

	for _, rename := range schemaDelta.RenamedColumns {
		if isKey(rename.OldName) {
			return fmt.Sprintf("key column %s was renamed to %s", rename.OldName, rename.NewName)
		}
	}
	for _, dropped := range schemaDelta.DroppedColumns {
		if isKey(dropped) {
			return fmt.Sprintf("key column %s was dropped", dropped)
		}
	}
	for _, changed := range schemaDelta.ChangedColumns {
		prev := prevColumns[prevName[changed.Name]]
		if prev == nil || prev.Type == changed.Type && prev.TypeModifier == changed.TypeModifier {
			continue // nullability only
		}
		if isKey(prev.Name) {
			return fmt.Sprintf("key column %s changed type from %s to %s", prev.Name, prev.Type, changed.Type)
		}
		if !isWideningTypeChange(types.QValueKind(prev.Type), types.QValueKind(changed.Type), prev.TypeModifier, changed.TypeModifier) {
			return fmt.Sprintf("column %s changed type from %s to %s", changed.Name, prev.Type, changed.Type)
		}
	}
	for _, added := range schemaDelta.AddedColumns {
		if attr, ok := attrs[added.Name]; ok && attr.hasDefault && !attr.hasMissing {
			return fmt.Sprintf("column %s was added with a volatile default, which rewrote existing rows", added.Name)
		}
	}

	// the key decides the ClickHouse ORDER BY and deduplication; read it the way mirror setup did
	schemaTable, err := common.ParseTableIdentifier(schemaDelta.SrcTableName)
	if err != nil {
		p.logger.Warn("cannot parse table name to check its key", slog.String("table", schemaDelta.SrcTableName))
		return ""
	}
	identity, err := p.getReplicaIdentityType(ctx, relID, schemaTable)
	if err != nil {
		p.logger.Warn("failed to read replica identity", slog.String("table", schemaDelta.SrcTableName), slog.Any("error", err))
		return ""
	}
	isFull := identity == ReplicaIdentityFull
	if isFull != prevSchema.IsReplicaIdentityFull {
		return fmt.Sprintf("replica identity changed to %c", identity)
	}
	keys, err := p.getUniqueColumns(ctx, relID, identity, schemaTable)
	if err != nil {
		p.logger.Warn("failed to read key columns", slog.String("table", schemaDelta.SrcTableName), slog.Any("error", err))
		return ""
	}
	if isFull && len(keys) == 0 {
		return "" // every column is the key, which column changes already cover
	}
	expected := make([]string, 0, len(prevSchema.PrimaryKeyColumns))
	for _, key := range prevSchema.PrimaryKeyColumns {
		name := key
		for _, rename := range schemaDelta.RenamedColumns {
			if rename.OldName == key {
				name = rename.NewName
			}
		}
		expected = append(expected, name)
	}
	if !slices.Equal(keys, expected) {
		return fmt.Sprintf("key changed from %v to %v", prevSchema.PrimaryKeyColumns, keys)
	}
	return ""
}

// backfillAttnums fills the attnums of catalog schemas stored before attnums were tracked, matching live
// source columns by name, in memory and in the catalog. A column renamed since its schema was stored keeps 0.
func (c *PostgresConnector) backfillAttnums(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	flowName string,
	schemas map[string]*protos.TableSchema,
) error {
	var tableNames []string
	var relIDs []uint32
	for tableName, schema := range schemas {
		if schema.GetTableOid() != 0 && slices.ContainsFunc(schema.Columns, func(col *protos.FieldDescription) bool {
			return col.Attnum == 0
		}) {
			tableNames = append(tableNames, tableName)
			relIDs = append(relIDs, schema.TableOid)
		}
	}
	if len(tableNames) == 0 {
		return nil
	}
	rows, err := c.conn.Query(ctx,
		"SELECT attrelid, attname, attnum FROM pg_attribute WHERE attrelid = ANY($1) AND attnum > 0 AND NOT attisdropped", relIDs)
	if err != nil {
		return fmt.Errorf("failed to query column attnums: %w", err)
	}
	attnums := make(map[uint32]map[string]int32)
	var relID uint32
	var name string
	var attnum int32
	if _, err := pgx.ForEachRow(rows, []any{&relID, &name, &attnum}, func() error {
		if attnums[relID] == nil {
			attnums[relID] = make(map[string]int32)
		}
		attnums[relID][name] = attnum
		return nil
	}); err != nil {
		return fmt.Errorf("failed to read column attnums: %w", err)
	}
	fill := func(schema *protos.TableSchema) {
		for _, col := range schema.Columns {
			if col.Attnum == 0 {
				col.Attnum = attnums[schema.TableOid][col.Name]
			}
		}
	}
	for _, tableName := range tableNames {
		fill(schemas[tableName])
	}
	return internal.ReadModifyWriteTableSchemasToCatalog(ctx, catalogPool, c.logger, flowName, tableNames,
		func(stored map[string]*protos.TableSchema) (map[string]*protos.TableSchema, error) {
			for _, schema := range stored {
				if schema != nil {
					fill(schema)
				}
			}
			return stored, nil
		})
}
