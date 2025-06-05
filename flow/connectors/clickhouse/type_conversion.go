package connclickhouse

import (
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func GetColumnsTypeConversion() (*protos.ColumnsTypeConversionResponse, error) {
	res := make([]*protos.ColumnsTypeConversion, 0)
	for qkind, destTypes := range clickhouse.ListSupportedTypeConversions() {
		res = append(res, &protos.ColumnsTypeConversion{
			Qkind:            string(qkind),
			DestinationTypes: destTypes,
		})
	}
	return &protos.ColumnsTypeConversionResponse{
		Conversions: res,
	}, nil
}

func findTypeConversions(schema types.QRecordSchema, columns []*protos.ColumnSetting) map[string]types.TypeConversion {
	typeConversions := make(map[string]types.TypeConversion)

	colNameToType := make(map[string]types.QValueKind, len(schema.Fields))
	for _, field := range schema.Fields {
		colNameToType[field.Name] = field.Type
	}

	for _, col := range columns {
		colType, exist := colNameToType[col.SourceName]
		if !exist {
			continue
		}
		conversions, exist := clickhouse.SupportedDestinationTypes[col.DestinationType]
		if !exist {
			continue
		}
		for _, conversion := range conversions {
			if conversion.FromKind() == colType {
				typeConversions[col.SourceName] = conversion
			}
		}
	}

	return typeConversions
}

func applyTypeConversions(schema types.QRecordSchema, typeConversions map[string]types.TypeConversion) types.QRecordSchema {
	for i, field := range schema.Fields {
		if conversion, exist := typeConversions[field.Name]; exist {
			schema.Fields[i] = conversion.SchemaConversion(field)
		}
	}
	return schema
}
