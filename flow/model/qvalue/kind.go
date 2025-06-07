package qvalue

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func getClickHouseTypeForNumericColumn(ctx context.Context, env map[string]string, typeModifier int32, isArray bool) (string, error) {
	if typeModifier == -1 {
		numericAsStringEnabled, err := internal.PeerDBEnableClickHouseNumericAsString(ctx, env)
		if err != nil {
			return "", err
		}
		if numericAsStringEnabled {
			if isArray {
				return "Array(String)", nil
			} else {
				return "String", nil
			}
		}
	} else if rawPrecision, _ := datatypes.ParseNumericTypmod(typeModifier); rawPrecision > datatypes.PeerDBClickHouseMaxPrecision {
		if isArray {
			return "Array(String)", nil
		} else {
			return "String", nil
		}
	}
	precision, scale := datatypes.GetNumericTypeForWarehouse(typeModifier, datatypes.ClickHouseNumericCompatibility{})
	prefix, suffix := "", ""
	if isArray {
		prefix, suffix = "Array(", ")"
	}
	return fmt.Sprintf("%sDecimal(%d, %d)%s", prefix, precision, scale, suffix), nil
}

func ToDWHColumnType(
	ctx context.Context,
	kind types.QValueKind,
	env map[string]string,
	dwhType protos.DBType,
	column *protos.FieldDescription,
	nullableEnabled bool,
) (string, error) {
	var colType string
	switch dwhType {
	case protos.DBType_SNOWFLAKE:
		if kind == types.QValueKindNumeric {
			precision, scale := datatypes.GetNumericTypeForWarehouse(column.TypeModifier, datatypes.SnowflakeNumericCompatibility{})
			colType = fmt.Sprintf("NUMERIC(%d,%d)", precision, scale)
		} else if val, ok := types.QValueKindToSnowflakeTypeMap[kind]; ok {
			colType = val
		} else {
			colType = "STRING"
		}
		if nullableEnabled && !column.Nullable {
			colType += " NOT NULL"
		}
	case protos.DBType_CLICKHOUSE:
		if kind == types.QValueKindNumeric {
			var err error
			colType, err = getClickHouseTypeForNumericColumn(ctx, env, column.TypeModifier, false)
			if err != nil {
				return "", err
			}
		} else if kind == types.QValueKindArrayNumeric {
			var err error
			colType, err = getClickHouseTypeForNumericColumn(ctx, env, column.TypeModifier, true)
			if err != nil {
				return "", err
			}
		} else if val, ok := types.QValueKindToClickHouseTypeMap[kind]; ok {
			colType = val
		} else {
			colType = "String"
		}
		if nullableEnabled && column.Nullable && !kind.IsArray() {
			if colType == "LowCardinality(String)" {
				colType = "LowCardinality(Nullable(String))"
			} else {
				colType = fmt.Sprintf("Nullable(%s)", colType)
			}
		}
	default:
		return "", fmt.Errorf("unknown dwh type: %v", dwhType)
	}
	return colType, nil
}
