package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// CustomDataType holds metadata for a PostgreSQL custom type (enum, composite, domain, array).
type CustomDataType struct {
	Name  string
	Type  byte
	Delim byte // non-zero for array types
}

// GetCustomDataTypes fetches all user-defined types from the PostgreSQL catalog.
func GetCustomDataTypes(ctx context.Context, conn *pgx.Conn) (map[uint32]CustomDataType, error) {
	rows, err := conn.Query(ctx, `
		SELECT t.oid, t.typname, coalesce(at.typtype, t.typtype), coalesce(at.typdelim, 0::"char")
		FROM pg_catalog.pg_type t
		LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
		LEFT JOIN pg_catalog.pg_class c ON c.oid = t.typrelid
		LEFT JOIN pg_catalog.pg_type at ON at.typarray = t.oid
		WHERE t.typrelid = 0 OR c.relkind = 'c'
		AND n.nspname NOT IN ('pg_catalog', 'information_schema');
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to get customTypeMapping: %w", err)
	}

	customTypeMap := map[uint32]CustomDataType{}
	var typeID pgtype.Uint32
	var cdt CustomDataType
	if _, err := pgx.ForEachRow(rows, []any{&typeID, &cdt.Name, &cdt.Type, &cdt.Delim}, func() error {
		customTypeMap[typeID.Uint32] = cdt
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to scan into custom type mapping: %w", err)
	}
	return customTypeMap, nil
}

// OID constants for types not covered by pgtype's built-in map.
const (
	MoneyOID        uint32 = 790
	TxidSnapshotOID uint32 = 2970
	TsvectorOID     uint32 = 3614
	TsqueryOID      uint32 = 3615
)

// OIDToName resolves a PostgreSQL type OID to its pg_type.typname string.
// It consults typeMap first, then falls back to a small set of well-known OIDs
// not covered by pgtype, and finally looks up customTypeMapping for enums/composites.
func OIDToName(typeMap *pgtype.Map, oid uint32, customTypeMapping map[uint32]CustomDataType) (string, error) {
	if ty, ok := typeMap.TypeForOID(oid); ok {
		return ty.Name, nil
	}
	// Workaround for types not defined by pgtype.
	switch oid {
	case pgtype.TimetzOID:
		return "timetz", nil
	case pgtype.XMLOID:
		return "xml", nil
	case MoneyOID:
		return "money", nil
	case TxidSnapshotOID:
		return "txid_snapshot", nil
	case TsvectorOID:
		return "tsvector", nil
	case TsqueryOID:
		return "tsquery", nil
	default:
		typeData, ok := customTypeMapping[oid]
		if !ok {
			return "", fmt.Errorf("error getting type name for %d", oid)
		}
		return typeData.Name, nil
	}
}
