package model

import (
	"encoding/json"
	"fmt"

	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

type QRecordAvroConverter struct {
	QRecord        *QRecord
	TargetDWH      qvalue.QDWHType
	NullableFields *map[string]bool
	ColNames       []string
}

func NewQRecordAvroConverter(
	q *QRecord,
	targetDWH qvalue.QDWHType,
	nullableFields *map[string]bool,
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
		nullable, ok := (*qac.NullableFields)[key]

		avroConverter := qvalue.NewQValueAvroConverter(
			qac.QRecord.Entries[idx],
			qac.TargetDWH,
			nullable && ok,
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
	NullableFields map[string]bool
}

func GetAvroSchemaDefinition(
	dstTableName string,
	qRecordSchema *QRecordSchema,
) (*QRecordAvroSchemaDefinition, error) {
	avroFields := []QRecordAvroField{}
	nullableFields := map[string]bool{}

	for _, qField := range qRecordSchema.Fields {
		avroType, err := qvalue.GetAvroSchemaFromQValueKind(qField.Type, qField.Nullable)
		if err != nil {
			return nil, err
		}

		consolidatedType := avroType.AvroLogicalSchema

		if avroType.RespectNull && qField.Nullable {
			consolidatedType = []interface{}{"null", consolidatedType}
			nullableFields[qField.Name] = true
		}

		avroFields = append(avroFields, QRecordAvroField{
			Name: qField.Name,
			Type: consolidatedType,
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
