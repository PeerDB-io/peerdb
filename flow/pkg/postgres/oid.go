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

	// for domain types
	BaseOID    uint32
	BaseTypmod int32
}

// GetCustomDataTypes fetches all types from the PostgreSQL catalog.
// pg_catalog stays included: pgtype's map does not know every built-in (regclass, pg_lsn, oidvector, ...) and OIDToName falls back here.
func GetCustomDataTypes(ctx context.Context, conn *pgx.Conn) (map[uint32]CustomDataType, error) {
	rows, err := conn.Query(ctx, `
		SELECT t.oid, t.typname, coalesce(at.typtype, t.typtype), coalesce(at.typdelim, 0::"char"), t.typbasetype, t.typtypmod
		FROM pg_catalog.pg_type t
		LEFT JOIN pg_catalog.pg_class c ON c.oid = t.typrelid
		LEFT JOIN pg_catalog.pg_type at ON at.typarray = t.oid
		WHERE t.typrelid = 0 OR c.relkind = 'c'
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to get customTypeMapping: %w", err)
	}

	customTypeMap := map[uint32]CustomDataType{}
	var typeID, baseTypeID pgtype.Uint32
	var cdt CustomDataType
	if _, err := pgx.ForEachRow(rows, []any{&typeID, &cdt.Name, &cdt.Type, &cdt.Delim, &baseTypeID, &cdt.BaseTypmod}, func() error {
		cdt.BaseOID = baseTypeID.Uint32
		customTypeMap[typeID.Uint32] = cdt
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to scan into custom type mapping: %w", err)
	}

	for oid, cdt := range customTypeMap {
		// domain types can be nested, so make sure the root type's oid and typmod are set
		for cdt.BaseOID != 0 {
			base, ok := customTypeMap[cdt.BaseOID]
			if !ok || base.BaseOID == 0 {
				break
			}
			cdt.BaseTypmod = base.BaseTypmod
			cdt.BaseOID = base.BaseOID
		}
		customTypeMap[oid] = cdt
	}
	return customTypeMap, nil
}

// ResolveDataType checks custom type mapping and resolves oid  and typmod to its base type for a domain type
func ResolveDataType(oid uint32, typmod int32, customTypeMapping map[uint32]CustomDataType) (uint32, int32) {
	typeData := customTypeMapping[oid]
	if isDomainType := typeData.BaseOID != 0; isDomainType {
		return typeData.BaseOID, typeData.BaseTypmod
	}
	return oid, typmod
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
