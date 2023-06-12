package model

import (
	"encoding/json"
	"fmt"
)

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

func GetAvroSchemaDefinition(dstTableName string, qRecordSchema *QRecordSchema) (*QRecordAvroSchemaDefinition, error) {
	avroFields := []QRecordAvroField{}
	nullableFields := map[string]bool{}

	for _, qField := range qRecordSchema.Fields {
		avroType, err := GetAvroType(qField)
		if err != nil {
			return nil, err
		}

		consolidatedType := avroType.AType

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

type avroType struct {
	AType       interface{}
	RespectNull bool
}

func GetAvroType(qField *QField) (*avroType, error) {
	switch qField.Type {
	case QValueKindString:
		return &avroType{
			AType:       "string",
			RespectNull: qField.Nullable,
		}, nil
	case QValueKindInt16, QValueKindInt32:
		return &avroType{
			AType:       "long",
			RespectNull: qField.Nullable,
		}, nil
	case QValueKindInt64:
		return &avroType{
			AType:       "long",
			RespectNull: qField.Nullable,
		}, nil
	case QValueKindFloat16, QValueKindFloat32:
		return &avroType{
			AType:       "float",
			RespectNull: qField.Nullable,
		}, nil
	case QValueKindFloat64:
		return &avroType{
			AType:       "double",
			RespectNull: qField.Nullable,
		}, nil
	case QValueKindBoolean:
		return &avroType{
			AType:       "boolean",
			RespectNull: qField.Nullable,
		}, nil
	case QValueKindBytes:
		return &avroType{
			AType:       "bytes",
			RespectNull: qField.Nullable,
		}, nil
	case QValueKindNumeric:
		// For the case of numeric values, you may need to be more specific depending
		// on the range and precision of your numeric data.
		return &avroType{
			AType: map[string]interface{}{
				"type":        "bytes",
				"logicalType": "decimal",
				"precision":   38,
				"scale":       9,
			},
			RespectNull: false,
		}, nil
	case QValueKindUUID:
		// treat UUID as a string
		return &avroType{
			AType:       "string",
			RespectNull: qField.Nullable,
		}, nil
	case QValueKindETime:
		return &avroType{
			AType: map[string]string{
				"type":        "long",
				"logicalType": "timestamp-millis",
			},
			RespectNull: false,
		}, nil
	case QValueKindJSON, QValueKindArray, QValueKindStruct:
		// Handling complex types like JSON, Array, and Struct may require more complex logic
		return nil, fmt.Errorf("complex types not supported yet: %s", qField.Type)
	case QValueKindBit:
		// Bit types may need their own specific handling or may not map directly to Avro types
		return nil, fmt.Errorf("unsupported QField type: %s", qField.Type)
	default:
		return nil, fmt.Errorf("unsupported QField type: %s", qField.Type)
	}
}
