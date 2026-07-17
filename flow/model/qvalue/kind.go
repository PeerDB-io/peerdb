package qvalue

import (
	"context"
	"fmt"
	"slices"

	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	peerdb_clickhouse "github.com/PeerDB-io/peerdb/flow/pkg/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type NumericDestinationType = peerdb_clickhouse.NumericDestinationType

func GetNumericDestinationType(
	precision, scale int16, targetDWH protos.DBType, unboundedNumericAsString bool,
) NumericDestinationType {
	if targetDWH == protos.DBType_CLICKHOUSE {
		return peerdb_clickhouse.GetNumericDestinationType(precision, scale, unboundedNumericAsString)
	}
	destPrecision, destScale := DetermineNumericSettingForDWH(precision, scale, targetDWH)
	return NumericDestinationType{
		IsString:  false,
		Precision: destPrecision,
		Scale:     destScale,
	}
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
		opts := peerdb_clickhouse.TypeMappingOptions{
			Time64Enabled:   slices.Contains(flags, shared.Flag_ClickHouseTime64Enabled),
			NullableEnabled: nullableEnabled,
		}
		// resolve dynamic settings only for the kinds that consult them
		switch kind {
		case types.QValueKindNumeric, types.QValueKindArrayNumeric:
			asString, err := internal.PeerDBEnableClickHouseNumericAsString(ctx, env)
			if err != nil {
				return "", err
			}
			opts.NumericAsString = asString
		case types.QValueKindJSON, types.QValueKindJSONB:
			opts.UseNativeJSONType = ShouldUseNativeJSONType(ctx, env, dwhVersion)
		}
		colType = peerdb_clickhouse.QValueKindToClickHouseType(kind, column.TypeModifier, column.Nullable, opts)
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
