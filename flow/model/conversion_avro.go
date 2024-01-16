package model

import (
	"encoding/json"
	"fmt"

	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

type QRecordAvroConverter struct {
	QRecord        QRecord
	TargetDWH      qvalue.QDWHType
	NullableFields map[string]struct{}
	ColNames       []string
}

func NewQRecordAvroConverter(
	q QRecord,
	targetDWH qvalue.QDWHType,
	nullableFields map[string]struct{},
	colNames []string,
) *QRecordAvroConverter {
	return &QRecordAvroConverter{
		QRecord:        q,
		TargetDWH:      targetDWH,
		NullableFields: nullableFields,
		ColNames:       colNames,
	}
}

func (qac *QRecordAvroConverter) Convert() (map[string]interface{}, error) {
	m := map[string]interface{}{}

	for idx := range qac.QRecord.Entries {
		key := qac.ColNames[idx]
		_, nullable := qac.NullableFields[key]

		avroConverter := qvalue.NewQValueAvroConverter(
			qac.QRecord.Entries[idx],
			qac.TargetDWH,
			nullable,
		)
		avroVal, err := avroConverter.ToAvroValue()
		if err != nil {
			return nil, fmt.Errorf("failed to convert QValue to Avro-compatible value: %v", err)
		}

		m[key] = avroVal
	}

	return m, nil
}

type QRecordAvroField struct {
	Name string      `json:"name"`
	Type interface{} `json:"type"`
}

type QRecordAvroSchema struct {
	Type   string             `json:"type"`
	Name   string             `json:"name"`
	Fields []QRecordAvroField `json:"fields"`
}

type QRecordAvroSchemaDefinition struct {
	Schema         string
	NullableFields map[string]struct{}
}

func GetAvroSchemaDefinition(
	dstTableName string,
	qRecordSchema *QRecordSchema,
	targetDWH qvalue.QDWHType,
) (*QRecordAvroSchemaDefinition, error) {
	avroFields := make([]QRecordAvroField, 0, len(qRecordSchema.Fields))
	nullableFields := make(map[string]struct{})

	for _, qField := range qRecordSchema.Fields {
		avroType, err := qvalue.GetAvroSchemaFromQValueKind(qField.Type, targetDWH)
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
		return nil, fmt.Errorf("failed to marshal Avro schema to JSON: %v", err)
	}

	return &QRecordAvroSchemaDefinition{
		Schema:         string(avroSchemaJSON),
		NullableFields: nullableFields,
	}, nil
}
