package qvalue

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/google/uuid"
	"github.com/linkedin/goavro"
)

// QValueKindAvroSchema defines a structure for representing Avro schemas.
// AvroLogicalSchema holds the Avro logical schema for a corresponding QValueKind.
type QValueKindAvroSchema struct {
	AvroLogicalSchema interface{}
}

// GetAvroSchemaFromQValueKind returns the Avro schema for a given QValueKind.
// The function takes in two parameters, a QValueKind and a boolean indicating if the
// Avro schema should respect null values. It returns a QValueKindAvroSchema object
// representing the Avro schema and an error if the QValueKind is unsupported.
//
// For example, QValueKindInt64 would return an AvroLogicalSchema of "long". Unsupported QValueKinds
// will return an error.
//
// The function currently does not support the following QValueKinds:
// - QValueKindJSON
// - QValueKindArray
// - QValueKindStruct
// - QValueKindBit
//
// Please note that for QValueKindNumeric and QValueKindETime, RespectNull is always
// set to false, regardless of the nullable value passed in.
func GetAvroSchemaFromQValueKind(kind QValueKind, nullable bool) (*QValueKindAvroSchema, error) {
	switch kind {
	case QValueKindString, QValueKindUUID:
		return &QValueKindAvroSchema{
			AvroLogicalSchema: "string",
		}, nil
	case QValueKindInt16, QValueKindInt32, QValueKindInt64:
		return &QValueKindAvroSchema{
			AvroLogicalSchema: "long",
		}, nil
	case QValueKindFloat16, QValueKindFloat32:
		return &QValueKindAvroSchema{
			AvroLogicalSchema: "float",
		}, nil
	case QValueKindFloat64:
		return &QValueKindAvroSchema{
			AvroLogicalSchema: "double",
		}, nil
	case QValueKindBoolean:
		return &QValueKindAvroSchema{
			AvroLogicalSchema: "boolean",
		}, nil
	case QValueKindBytes:
		return &QValueKindAvroSchema{
			AvroLogicalSchema: "bytes",
		}, nil
	case QValueKindNumeric:
		return &QValueKindAvroSchema{
			AvroLogicalSchema: map[string]interface{}{
				"type":        "bytes",
				"logicalType": "decimal",
				"precision":   38,
				"scale":       9,
			},
		}, nil
	case QValueKindETime:
		return &QValueKindAvroSchema{
			AvroLogicalSchema: map[string]string{
				"type":        "long",
				"logicalType": "timestamp-micros",
			},
		}, nil
	case QValueKindJSON, QValueKindArray, QValueKindStruct, QValueKindBit:
		return nil, fmt.Errorf("complex or unsupported types: %s", kind)
	default:
		return nil, errors.New("unsupported QValueKind type")
	}
}

type QValueAvroConverter struct {
	Value     *QValue
	TargetDWH QDWHType
	Nullable  bool
}

func NewQValueAvroConverter(value *QValue, targetDWH QDWHType, nullable bool) *QValueAvroConverter {
	return &QValueAvroConverter{
		Value:     value,
		TargetDWH: targetDWH,
		Nullable:  nullable,
	}
}

func (c *QValueAvroConverter) ToAvroValue() (interface{}, error) {
	switch c.Value.Kind {
	case QValueKindInvalid:
		return nil, fmt.Errorf("invalid QValueKind: %v", c.Value)
	case QValueKindETime:
		t, err := c.processExtendedTime()
		if err != nil || t == nil {
			return t, err
		}
		return goavro.Union("long.timestamp-micros", t), nil
	case QValueKindString:
		return c.processNullableUnion("string", c.Value.Value)
	case QValueKindFloat16, QValueKindFloat32, QValueKindFloat64:
		return c.processNullableUnion("double", c.Value.Value)
	case QValueKindInt16, QValueKindInt32, QValueKindInt64:
		return c.processNullableUnion("long", c.Value.Value)
	case QValueKindBoolean:
		return c.processNullableUnion("boolean", c.Value.Value)
	case QValueKindArray:
		return nil, fmt.Errorf("QValueKindArray not supported")
	case QValueKindStruct:
		return nil, fmt.Errorf("QValueKindStruct not supported")
	case QValueKindNumeric:
		return c.processNumeric()
	case QValueKindBytes:
		return c.processBytes()
	case QValueKindJSON:
		jsonString, ok := c.Value.Value.(string)
		if !ok {
			return nil, fmt.Errorf("invalid JSON value")
		}
		return c.processNullableUnion("string", jsonString)
	case QValueKindUUID:
		return c.processUUID()
	default:
		return nil, fmt.Errorf("unsupported QValueKind: %s", c.Value.Kind)
	}
}

func (c *QValueAvroConverter) processExtendedTime() (interface{}, error) {
	et, ok := c.Value.Value.(*ExtendedTime)
	if !ok {
		return nil, fmt.Errorf("invalid ExtendedTime value")
	}

	if et == nil {
		return nil, nil
	}

	switch et.NestedKind.Type {
	case DateTimeKindType:
		ret := et.Time.UnixMicro()
		// Snowflake has issues with avro timestamp types
		// See: https://stackoverflow.com/questions/66104762/snowflake-date-column-have-incorrect-date-from-avro-file
		if c.TargetDWH == QDWHTypeSnowflake {
			ret = ret / 1000000
		}
		return ret, nil
	case DateKindType:
		ret := et.Time.Format("2006-01-02")
		return ret, nil
	case TimeKindType:
		ret := et.Time.Format("15:04:05.999999")
		return ret, nil
	default:
		return nil, fmt.Errorf("unsupported ExtendedTimeKindType: %s", et.NestedKind.Type)
	}
}

func (c *QValueAvroConverter) processNullableUnion(
	avroType string,
	value interface{},
) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	if c.Nullable {
		return goavro.Union(avroType, value), nil
	}

	return value, nil
}

func (c *QValueAvroConverter) processNumeric() (interface{}, error) {
	if c.Value.Value == nil && c.Nullable {
		return nil, nil
	}

	num, ok := c.Value.Value.(*big.Rat)
	if !ok {
		return nil, fmt.Errorf("invalid Numeric value: expected *big.Rat, got %T", c.Value.Value)
	}

	if c.Nullable {
		return goavro.Union("bytes.decimal", num), nil
	}

	return num, nil
}

func (c *QValueAvroConverter) processBytes() (interface{}, error) {
	byteData, ok := c.Value.Value.([]byte)
	if !ok {
		return nil, fmt.Errorf("invalid Bytes value")
	}

	if c.Nullable {
		return goavro.Union("bytes", byteData), nil
	}

	return byteData, nil
}

func (c *QValueAvroConverter) processUUID() (interface{}, error) {
	if c.Value.Value == nil {
		return nil, nil
	}

	byteData, ok := c.Value.Value.([16]byte)
	if !ok {
		return nil, fmt.Errorf("invalid UUID value %v", c.Value.Value)
	}

	u, err := uuid.FromBytes(byteData[:])
	if err != nil {
		return nil, fmt.Errorf("conversion of invalid UUID value: %v", err)
	}

	uuidString := u.String()

	if c.Nullable {
		return goavro.Union("string", uuidString), nil
	}

	return uuidString, nil
}
