package clickhouse

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/pkg/datatypes"
	"github.com/PeerDB-io/peerdb/flow/pkg/postgres"
	"github.com/PeerDB-io/peerdb/flow/pkg/types"
)

// TypeMappingOptions carries the destination-side state and resolved dynamic
// settings that influence the ClickHouse column type. Resolving them is the
// caller's job: dynamic-setting lookup lives in the flow module, and the
// time64/JSON capabilities depend on the destination ClickHouse instance.
type TypeMappingOptions struct {
	// PEERDB_CLICKHOUSE_UNBOUNDED_NUMERIC_AS_STRING; maps unbounded NUMERIC columns to String
	NumericAsString bool
	// use the native JSON type for JSON columns; requires ClickHouse >= 25.3
	// and PEERDB_CLICKHOUSE_ENABLE_JSON
	UseNativeJSONType bool
	// destination has the enable_time_time64_type setting on; maps time-of-day
	// columns to Time64(6) instead of DateTime64(6)
	Time64Enabled bool
	// mirror- or column-level nullable setting; wraps nullable columns in Nullable(...)
	NullableEnabled bool
}

type NumericDestinationType struct {
	IsString         bool
	Precision, Scale int16
}

// GetNumericDestinationType decides how a numeric with the given
// precision/scale lands in ClickHouse: Decimal(p, s), or String when the
// precision exceeds what ClickHouse supports (or for unbounded numerics when
// numericAsString is set).
func GetNumericDestinationType(precision, scale int16, numericAsString bool) NumericDestinationType {
	if precision == 0 && scale == 0 && numericAsString {
		return NumericDestinationType{IsString: true}
	}
	if precision > datatypes.PeerDBClickHouseMaxPrecision {
		return NumericDestinationType{IsString: true}
	}
	destPrecision, destScale := datatypes.GetNumericTypeForWarehousePrecisionScale(
		precision, scale, datatypes.ClickHouseNumericCompatibility{})
	return NumericDestinationType{
		IsString:  false,
		Precision: destPrecision,
		Scale:     destScale,
	}
}

func numericColumnType(typeModifier int32, numericAsString bool) string {
	precision, scale := common.ParseNumericTypmod(typeModifier)
	destinationType := GetNumericDestinationType(precision, scale, numericAsString)
	if destinationType.IsString {
		return "String"
	}
	return fmt.Sprintf("Decimal(%d, %d)", destinationType.Precision, destinationType.Scale)
}

// QValueKindToClickHouseType maps an internal QValueKind to the destination
// ClickHouse column type. typeModifier is the Postgres-style type modifier of
// the source column (used for numerics), nullable its declared nullability.
func QValueKindToClickHouseType(
	kind types.QValueKind,
	typeModifier int32,
	nullable bool,
	opts TypeMappingOptions,
) string {
	var colType string
	switch {
	case kind == types.QValueKindNumeric:
		colType = numericColumnType(typeModifier, opts.NumericAsString)
	case kind == types.QValueKindArrayNumeric:
		colType = fmt.Sprintf("Array(%s)", numericColumnType(typeModifier, opts.NumericAsString))
	case (kind == types.QValueKindJSON || kind == types.QValueKindJSONB) && opts.UseNativeJSONType:
		colType = "JSON"
	case (kind == types.QValueKindTime || kind == types.QValueKindTimeTZ) && opts.Time64Enabled:
		colType = "Time64(6)"
	default:
		if val, ok := types.QValueKindToClickHouseTypeMap[kind]; ok {
			colType = val
		} else {
			colType = "String"
		}
	}
	if opts.NullableEnabled && nullable && !kind.IsArray() {
		if colType == "LowCardinality(String)" {
			colType = "LowCardinality(Nullable(String))"
		} else {
			colType = fmt.Sprintf("Nullable(%s)", colType)
		}
	}
	return colType
}

// PostgresTypeToClickHouseType translates a Postgres column type (by OID and
// type modifier) to the destination ClickHouse column type.
//
// The returned type is always usable: unrecognized Postgres types fall back
// the same way replication does (typically String). The error, when non-nil,
// only reports the name of an unrecognized type so callers can warn about it.
func PostgresTypeToClickHouseType(
	recvOID uint32,
	typeModifier int32,
	nullable bool,
	customTypeMapping map[uint32]postgres.CustomDataType,
	typeMap *pgtype.Map,
	internalVersion uint32,
	opts TypeMappingOptions,
) (string, error) {
	kind, err := postgres.PostgresOIDToQValueKind(recvOID, customTypeMapping, typeMap, internalVersion)
	return QValueKindToClickHouseType(kind, typeModifier, nullable, opts), err
}
