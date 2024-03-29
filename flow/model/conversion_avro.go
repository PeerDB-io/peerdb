package model

import (
	"encoding/json"
	"fmt"

	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

type QRecordAvroConverter struct {
	logger    log.Logger
	QRecord   []qvalue.QValue
	ColNames  []string
	TargetDWH protos.DBType
}

func NewQRecordAvroConverter(
	q []qvalue.QValue,
	targetDWH protos.DBType,
	colNames []string,
	logger log.Logger,
) *QRecordAvroConverter {
	return &QRecordAvroConverter{
		QRecord:   q,
		TargetDWH: targetDWH,
		ColNames:  colNames,
		logger:    logger,
	}
}

func (qac *QRecordAvroConverter) Convert(schema *QRecordAvroSchemaDefinition) (map[string]interface{}, error) {
	m := make(map[string]interface{}, len(qac.QRecord))

	for idx, val := range qac.QRecord {
		key := qac.ColNames[idx]

		avroVal, err := qvalue.QValueToAvro(
			val,
			&schema.Fields[idx],
			qac.TargetDWH,
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
	Schema string
	Fields []qvalue.QField
}

func GetAvroSchemaDefinition(
	dstTableName string,
	qRecordSchema *qvalue.QRecordSchema,
	targetDWH protos.DBType,
) (*QRecordAvroSchemaDefinition, error) {
	avroFields := make([]QRecordAvroField, 0, len(qRecordSchema.Fields))

	for _, qField := range qRecordSchema.Fields {
		avroType, err := qvalue.GetAvroSchemaFromQValueKind(qField.Type, targetDWH, qField.Precision, qField.Scale)
		if err != nil {
			return nil, err
		}

		if qField.Nullable {
			avroType = []interface{}{"null", avroType}
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
		Schema: string(avroSchemaJSON),
		Fields: qRecordSchema.Fields,
	}, nil
}
