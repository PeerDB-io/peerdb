package qvalue

import (
	"context"
	"fmt"
	"slices"

	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type NumericDestinationType struct {
	IsString         bool
	Precision, Scale int16
}

func GetNumericDestinationType(
	precision, scale int16, targetDWH protos.DBType, unboundedNumericAsString bool,
) NumericDestinationType {
	if targetDWH == protos.DBType_CLICKHOUSE {
		if precision == 0 && scale == 0 && unboundedNumericAsString {
			return NumericDestinationType{IsString: true}
		}
		if precision > datatypes.PeerDBClickHouseMaxPrecision {
			return NumericDestinationType{IsString: true}
		}
	}
	destPrecision, destScale := DetermineNumericSettingForDWH(precision, scale, targetDWH)
	return NumericDestinationType{
		IsString:  false,
		Precision: destPrecision,
		Scale:     destScale,
	}
}

func getClickHouseTypeForNumericColumn(ctx context.Context, env map[string]string, typeModifier int32) (string, error) {
	precision, scale := datatypes.ParseNumericTypmod(typeModifier)
	asString, err := internal.PeerDBEnableClickHouseNumericAsString(ctx, env)
	if err != nil {
		return "", err
	}
	destinationType := GetNumericDestinationType(precision, scale, protos.DBType_CLICKHOUSE, asString)
	if destinationType.IsString {
		return "String", nil
	}
	return fmt.Sprintf("Decimal(%d, %d)", destinationType.Precision, destinationType.Scale), nil
}

func ToDWHColumnType(
	ctx context.Context,
	kind types.QValueKind,
	env map[string]string,
	dwhType protos.DBType,
	dwhVersion *chproto.Version,
	column *protos.FieldDescription,
	nullableEnabled bool,
	flags []string,
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
			colType, err = getClickHouseTypeForNumericColumn(ctx, env, column.TypeModifier)
			if err != nil {
				return "", err
			}
		} else if kind == types.QValueKindArrayNumeric {
			var err error
			colType, err = getClickHouseTypeForNumericColumn(ctx, env, column.TypeModifier)
			if err != nil {
				return "", err
			}
			colType = fmt.Sprintf("Array(%s)", colType)
		} else if (kind == types.QValueKindJSON || kind == types.QValueKindJSONB) && ShouldUseNativeJSONType(ctx, env, dwhVersion) {
			colType = "JSON"
		} else if (kind == types.QValueKindTime || kind == types.QValueKindTimeTZ) &&
			slices.Contains(flags, shared.Flag_ClickHouseTime64Enabled) {
			colType = "Time64(6)"
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

func ShouldUseNativeJSONType(ctx context.Context, env map[string]string, chVersion *chproto.Version) bool {
	if chVersion == nil {
		return false
	}
	// JSON data type is marked as production ready in version ClickHouse 25.3
	isJsonSupported := chproto.CheckMinVersion(chproto.Version{Major: 25, Minor: 3, Patch: 0}, *chVersion)
	// Treat error the same as not enabled
	isJsonEnabled, _ := internal.PeerDBEnableClickHouseJSON(ctx, env)
	return isJsonSupported && isJsonEnabled
}
