package model

import (
	"encoding/json"
	"fmt"

	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

type QRecordAvroConverter struct {
	logger         log.Logger
	QRecord        []qvalue.QValue
	NullableFields map[string]struct{}
	ColNames       []string
	TargetDWH      protos.DBType
}

func NewQRecordAvroConverter(
	q []qvalue.QValue,
	targetDWH protos.DBType,
	nullableFields map[string]struct{},
	colNames []string,
	logger log.Logger,
) *QRecordAvroConverter {
	return &QRecordAvroConverter{
		QRecord:        q,
		TargetDWH:      targetDWH,
		NullableFields: nullableFields,
		ColNames:       colNames,
		logger:         logger,
	}
}

func (qac *QRecordAvroConverter) Convert() (map[string]interface{}, error) {
	m := make(map[string]interface{}, len(qac.QRecord))

	for idx, val := range qac.QRecord {
		key := qac.ColNames[idx]
		_, nullable := qac.NullableFields[key]

		avroVal, err := qvalue.QValueToAvro(
			val,
			qac.TargetDWH,
			nullable,
			qac.logger,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to convert QValue to Avro-compatible value: %w", err)
		}

		m[key] = avroVal
	}

	return m, nil
}

type QRecordAvroField struct {
	Type interface{} `json:"type"`
	Name string      `json:"name"`
}

type QRecordAvroSchema struct {
	Type   string             `json:"type"`
	Name   string             `json:"name"`
	Fields []QRecordAvroField `json:"fields"`
}

type QRecordAvroSchemaDefinition struct {
	NullableFields map[string]struct{}
	Schema         string
}

func GetAvroSchemaDefinition(
	dstTableName string,
	qRecordSchema *QRecordSchema,
	targetDWH protos.DBType,
) (*QRecordAvroSchemaDefinition, error) {
	avroFields := make([]QRecordAvroField, 0, len(qRecordSchema.Fields))
	nullableFields := make(map[string]struct{})

	for _, qField := range qRecordSchema.Fields {
		avroType, err := qvalue.GetAvroSchemaFromQValueKind(qField.Type, targetDWH, qField.Precision, qField.Scale)
		if err != nil {
			return nil, err
		}

		if qField.Nullable {
			avroType = []interface{}{"null", avroType}
			nullableFields[qField.Name] = struct{}{}
		}

		avroFields = append(avroFields, QRecordAvroField{
			Name: qField.Name,
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
		Schema:         string(avroSchemaJSON),
		NullableFields: nullableFields,
	}, nil
}
