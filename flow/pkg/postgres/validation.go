package postgres

import (
	"context"
	"fmt"
	"slices"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// ColumnSchema holds the PostgreSQL type information for a single column,
// as returned by FieldDescriptions after a "SELECT * FROM table LIMIT 0" query.
type ColumnSchema struct {
	TypeName string
	TypeMod  int32
}

// integerRank is used for integer promotion checks (higher = wider type).
var integerRank = map[string]int{
	"int2": 1,
	"int4": 2,
	"int8": 3,
}

// This is to reverse what make_numeric_typmod of Postgres does:
// https://github.com/postgres/postgres/blob/21912e3c0262e2cfe64856e028799d6927862563/src/backend/utils/adt/numeric.c#L897
func ParseNumericTypmod(typmod int32) (int16, int16) {
	if typmod == -1 {
		return 0, 0
	}
	const varhdrsz = int32(4)
	offsetMod := typmod - varhdrsz
	precision := int16((offsetMod >> 16) & 0x7FFF)
	scale := int16(offsetMod & 0x7FFF)
	return precision, scale
}

// CheckSchemaExists returns an error if the given schema does not exist in the database.
func CheckSchemaExists(ctx context.Context, conn *pgx.Conn, schema string) error {
	var exists bool
	err := conn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = $1)", schema).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if schema %s exists: %w", schema, err)
	}
	if !exists {
		return fmt.Errorf("schema %q does not exist on destination, PeerDB will not create it automatically", schema)
	}
	return nil
}

// GetDestinationTableSchema queries a PostgreSQL table's column type information
// using SELECT * FROM tableIdentifier LIMIT 0 to get field descriptions with OIDs.
// Returns pgx.ErrNoRows if the table exists but has no columns.
func GetDestinationTableSchema(ctx context.Context, conn *pgx.Conn, tableIdentifier string) (map[string]ColumnSchema, error) {
	parsedTable, err := common.ParseTableIdentifier(tableIdentifier)
	if err != nil {
		return nil, fmt.Errorf("invalid table identifier %s: %w", tableIdentifier, err)
	}

	customTypeMapping, err := GetCustomDataTypes(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch custom type mapping: %w", err)
	}

	rows, err := conn.Query(ctx,
		"SELECT * FROM "+parsedTable.String()+" LIMIT 0",
		pgx.QueryExecModeSimpleProtocol,
	)
	if err != nil {
		return nil, err
	}
	fields := slices.Clone(rows.FieldDescriptions())
	rows.Close()

	if len(fields) == 0 {
		return nil, pgx.ErrNoRows
	}

	return BuildColumnSchemaMap(fields, conn.TypeMap(), customTypeMapping)
}

// BuildColumnSchemaMap assembles a ColumnSchema map from pgx FieldDescriptions.
func BuildColumnSchemaMap(
	fields []pgconn.FieldDescription,
	typeMap *pgtype.Map,
	customTypeMapping map[uint32]CustomDataType,
) (map[string]ColumnSchema, error) {
	columns := make(map[string]ColumnSchema, len(fields))
	for _, fd := range fields {
		typeName, err := OIDToName(typeMap, fd.DataTypeOID, customTypeMapping)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve type for column %s: %w", fd.Name, err)
		}
		columns[fd.Name] = ColumnSchema{TypeName: typeName, TypeMod: fd.TypeModifier}
	}
	return columns, nil
}

// ColumnMapping holds a source→destination column name pair for rename resolution.
type ColumnMapping struct {
	SourceName      string
	DestinationName string
}

// ResolveDestinationColumnName returns the destination column name for srcColName,
// applying the first matching rename from mappings. Falls back to srcColName if none match.
func ResolveDestinationColumnName(srcColName string, mappings []ColumnMapping) string {
	for _, m := range mappings {
		if m.SourceName == srcColName && m.DestinationName != "" {
			return m.DestinationName
		}
	}
	return srcColName
}

// CheckColumnExists returns an error if dstColName is not present in dstColumns.
func CheckColumnExists(srcColName, dstColName string, dstColumns map[string]ColumnSchema, dstTableIdentifier string) error {
	if _, exists := dstColumns[dstColName]; !exists {
		return fmt.Errorf("source column %s (as %s) not found in destination table %s",
			srcColName, dstColName, dstTableIdentifier)
	}
	return nil
}

// CheckColumnTypeCompatibility returns an error if the destination column type
// is not compatible with the source column type.
//
// Compatibility rules (using pg_type.typname values, e.g. "int4", "varchar"):
//   - Exact type match: always compatible, subject to typmod rules below.
//   - varchar / bpchar → text: always compatible (text is a superset).
//   - Integer promotion: destination may be a wider integer (int2 → int4 → int8).
//   - varchar / bpchar same type: unbounded source into bounded destination is
//     rejected; bounded source into narrower destination is rejected.
//   - numeric same type: both sides constrained with different precision/scale → rejected;
//     either side unbounded (typmod == -1) → compatible.
func CheckColumnTypeCompatibility(
	srcColName, srcType string, srcTypeMod int32,
	dstColName, dstType string, dstTypeMod int32,
	dstTableIdentifier string,
) error {
	if srcType != dstType {
		// varchar/bpchar source → text destination is always a superset
		if (srcType == "varchar" || srcType == "bpchar") && dstType == "text" {
			return nil
		}
		// Integer promotion: destination must be wider or equal rank
		if srcRank, ok := integerRank[srcType]; ok {
			if dstRank, ok := integerRank[dstType]; ok && dstRank >= srcRank {
				return nil
			}
		}
		return fmt.Errorf(
			"source column %s type %q does not match destination column %s type %q in table %s",
			srcColName, srcType, dstColName, dstType, dstTableIdentifier,
		)
	}

	// Same base type — check type modifier where relevant.
	switch srcType {
	case "varchar", "bpchar":
		// srcTypeMod == -1 means the source is unbounded; destination must also be
		// unbounded to safely receive it.
		if srcTypeMod == -1 && dstTypeMod != -1 {
			return fmt.Errorf(
				"source column %s is unbounded %s but destination column %s has a length constraint in table %s",
				srcColName, srcType, dstColName, dstTableIdentifier,
			)
		}
		// Both bounded: destination must be at least as wide as source.
		// TypeModifier encodes length as (N + 4):
		// https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/varchar.c#L65
		//  so direct comparison is valid.
		if srcTypeMod != -1 && dstTypeMod != -1 && dstTypeMod < srcTypeMod {
			return fmt.Errorf(
				"source column %s %s(%d) is wider than destination column %s %s(%d) in table %s",
				srcColName, srcType, srcTypeMod-4, dstColName, dstType, dstTypeMod-4, dstTableIdentifier,
			)
		}
	case "numeric":
		// Both sides constrained and different → reject.
		if srcTypeMod != -1 && dstTypeMod != -1 && srcTypeMod != dstTypeMod {
			srcPrec, srcScale := ParseNumericTypmod(srcTypeMod)
			dstPrec, dstScale := ParseNumericTypmod(dstTypeMod)
			return fmt.Errorf(
				"source column %s numeric(%d,%d) does not match destination column %s numeric(%d,%d) in table %s",
				srcColName, srcPrec, srcScale, dstColName, dstPrec, dstScale, dstTableIdentifier,
			)
		}
	}
	return nil
}
