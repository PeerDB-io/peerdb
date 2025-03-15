package model

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
)

type QRecordAvroConverter struct {
	logger                   log.Logger
	Schema                   *QRecordAvroSchemaDefinition
	ColNames                 []string
	TargetDWH                protos.DBType
	UnboundedNumericAsString bool
}

func NewQRecordAvroConverter(
	ctx context.Context,
	env map[string]string,
	schema *QRecordAvroSchemaDefinition,
	targetDWH protos.DBType,
	colNames []string,
	logger log.Logger,
) (*QRecordAvroConverter, error) {
	var unboundedNumericAsString bool
	if targetDWH == protos.DBType_CLICKHOUSE {
		var err error
		unboundedNumericAsString, err = internal.PeerDBEnableClickHouseNumericAsString(ctx, env)
		if err != nil {
			return nil, err
		}
	}

	return &QRecordAvroConverter{
		Schema:                   schema,
		TargetDWH:                targetDWH,
		ColNames:                 colNames,
		logger:                   logger,
		UnboundedNumericAsString: unboundedNumericAsString,
	}, nil
}

func (qac *QRecordAvroConverter) Convert(ctx context.Context, env map[string]string, qrecord []qvalue.QValue) (map[string]any, error) {
	m := make(map[string]any, len(qrecord))
	for idx, val := range qrecord {
		avroVal, err := qvalue.QValueToAvro(
			ctx, env, val,
			&qac.Schema.Fields[idx], qac.TargetDWH, qac.logger, qac.UnboundedNumericAsString,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to convert QValue to Avro-compatible value: %w", err)
		}

		m[qac.ColNames[idx]] = avroVal
	}

	return m, nil
}

type QRecordAvroField struct {
	Type any    `json:"type"`
	Name string `json:"name"`
}

type QRecordAvroSchema struct {
	Type   string             `json:"type"`
	Name   string             `json:"name"`
	Fields []QRecordAvroField `json:"fields"`
}

type QRecordAvroSchemaDefinition struct {
	Schema string
	Fields []qvalue.QField
}

func GetAvroSchemaDefinition(
	ctx context.Context,
	env map[string]string,
	dstTableName string,
	qRecordSchema qvalue.QRecordSchema,
	targetDWH protos.DBType,
	avroNameMap map[string]string,
) (*QRecordAvroSchemaDefinition, error) {
	avroFields := make([]QRecordAvroField, 0, len(qRecordSchema.Fields))

	for _, qField := range qRecordSchema.Fields {
		avroType, err := qvalue.GetAvroSchemaFromQValueKind(ctx, env, qField.Type, targetDWH, qField.Precision, qField.Scale)
		if err != nil {
			return nil, err
		}

		if qField.Nullable {
			avroType = []any{"null", avroType}
		}

		avroFieldName := qField.Name
		if avroNameMap != nil {
			avroFieldName = avroNameMap[qField.Name]
		}

		avroFields = append(avroFields, QRecordAvroField{
			Name: avroFieldName,
			Type: avroType,
		})
	}

	avroSchema := QRecordAvroSchema{
		Type:   "record",
		Name:   dstTableName,
		Fields: avroFields,
	}

	avroSchemaJSON, err := json.Marshal(avroSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal Avro schema to JSON: %w", err)
	}

	return &QRecordAvroSchemaDefinition{
		Schema: string(avroSchemaJSON),
		Fields: qRecordSchema.Fields,
	}, nil
}

func ConstructColumnNameAvroFieldMap(fields []qvalue.QField) map[string]string {
	m := make(map[string]string, len(fields))
	for i, field := range fields {
		m[field.Name] = qvalue.ConvertToAvroCompatibleName(field.Name) + "_" + strconv.FormatInt(int64(i), 10)
	}
	return m
}
