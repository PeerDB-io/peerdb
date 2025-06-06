package qvalue

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func getClickHouseTypeForNumericColumn(ctx context.Context, env map[string]string, column *protos.FieldDescription) (string, error) {
	if column.TypeModifier == -1 {
		numericAsStringEnabled, err := internal.PeerDBEnableClickHouseNumericAsString(ctx, env)
		if err != nil {
			return "", err
		}
		if numericAsStringEnabled {
			return "String", nil
		}
	} else if rawPrecision, _ := datatypes.ParseNumericTypmod(column.TypeModifier); rawPrecision > datatypes.PeerDBClickHouseMaxPrecision {
		return "String", nil
	}
	precision, scale := datatypes.GetNumericTypeForWarehouse(column.TypeModifier, datatypes.ClickHouseNumericCompatibility{})
	return fmt.Sprintf("Decimal(%d, %d)", precision, scale), nil
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
			colType, err = getClickHouseTypeForNumericColumn(ctx, env, column)
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
