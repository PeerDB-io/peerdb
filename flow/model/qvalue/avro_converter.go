package qvalue

import (
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"
	"github.com/linkedin/goavro/v2"
	log "github.com/sirupsen/logrus"
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
	case QValueKindGeometry, QValueKindGeography, QValueKindPoint:
		return &QValueKindAvroSchema{
			AvroLogicalSchema: "string",
		}, nil
	case QValueKindInt16, QValueKindInt32, QValueKindInt64:
		return &QValueKindAvroSchema{
			AvroLogicalSchema: "long",
		}, nil
	case QValueKindFloat32:
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
	case QValueKindBytes, QValueKindBit:
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
	case QValueKindTime, QValueKindTimeTZ, QValueKindDate, QValueKindTimestamp, QValueKindTimestampTZ:
		return &QValueKindAvroSchema{
			AvroLogicalSchema: map[string]string{
				"type": "string",
			},
		}, nil
	case QValueKindHStore, QValueKindJSON, QValueKindStruct:
		return &QValueKindAvroSchema{
			AvroLogicalSchema: map[string]interface{}{
				"type":   "string",
				"values": "string",
			},
		}, nil
	case QValueKindArrayFloat32:
		return &QValueKindAvroSchema{
			AvroLogicalSchema: map[string]interface{}{
				"type":  "array",
				"items": "float",
			},
		}, nil
	case QValueKindArrayFloat64:
		return &QValueKindAvroSchema{
			AvroLogicalSchema: map[string]interface{}{
				"type":  "array",
				"items": "double",
			},
		}, nil
	case QValueKindArrayInt32:
		return &QValueKindAvroSchema{
			AvroLogicalSchema: map[string]interface{}{
				"type":  "array",
				"items": "int",
			},
		}, nil
	case QValueKindArrayInt64:
		return &QValueKindAvroSchema{
			AvroLogicalSchema: map[string]interface{}{
				"type":  "array",
				"items": "long",
			},
		}, nil
	case QValueKindArrayString:
		return &QValueKindAvroSchema{
			AvroLogicalSchema: map[string]interface{}{
				"type":  "array",
				"items": "string",
			},
		}, nil
	case QValueKindInvalid:
		// lets attempt to do invalid as a string
		return &QValueKindAvroSchema{
			AvroLogicalSchema: "string",
		}, nil
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
		// we will attempt to convert invalid to a string
		return c.processNullableUnion("string", c.Value.Value)
	case QValueKindTime, QValueKindTimeTZ, QValueKindDate, QValueKindTimestamp, QValueKindTimestampTZ:
		t, err := c.processGoTime()
		if err != nil || t == nil {
			return t, err
		}
		if c.TargetDWH == QDWHTypeSnowflake {
			if c.Nullable {
				return c.processNullableUnion("string", t.(string))
			} else {
				return t.(string), nil
			}
		}
		if c.Nullable {
			return goavro.Union("long.timestamp-micros", t.(int64)), nil
		} else {
			return t.(int64), nil
		}
	case QValueKindString:
		if c.TargetDWH == QDWHTypeSnowflake && c.Value.Value != nil &&
			(len(c.Value.Value.(string)) > 15*1024*1024) {
			log.Warn("Truncating TEXT value > 15MB for Snowflake!")
			log.Warn("Check this issue for details: https://github.com/PeerDB-io/peerdb/issues/309")
			return c.processNullableUnion("string", "")
		}
		return c.processNullableUnion("string", c.Value.Value)
	case QValueKindFloat32:
		return c.processNullableUnion("float", c.Value.Value)
	case QValueKindFloat64:
		if c.TargetDWH == QDWHTypeSnowflake || c.TargetDWH == QDWHTypeBigQuery {
			if f32Val, ok := c.Value.Value.(float32); ok {
				return c.processNullableUnion("double", float64(f32Val))
			}
		}
		return c.processNullableUnion("double", c.Value.Value)
	case QValueKindInt16, QValueKindInt32, QValueKindInt64:
		return c.processNullableUnion("long", c.Value.Value)
	case QValueKindBoolean:
		return c.processNullableUnion("boolean", c.Value.Value)
	case QValueKindStruct:
		return nil, fmt.Errorf("QValueKindStruct not supported")
	case QValueKindNumeric:
		return c.processNumeric()
	case QValueKindBytes, QValueKindBit:
		return c.processBytes()
	case QValueKindJSON:
		return c.processJSON()
	case QValueKindHStore:
		return nil, fmt.Errorf("QValueKindHStore not supported")
	case QValueKindArrayFloat32:
		return c.processArrayFloat32()
	case QValueKindArrayFloat64:
		return c.processArrayFloat64()
	case QValueKindArrayInt32:
		return c.processArrayInt32()
	case QValueKindArrayInt64:
		return c.processArrayInt64()
	case QValueKindArrayString:
		return c.processArrayString()
	case QValueKindUUID:
		return c.processUUID()
	case QValueKindGeography, QValueKindGeometry, QValueKindPoint:
		return c.processGeospatial()
	default:
		return nil, fmt.Errorf("[toavro] unsupported QValueKind: %s", c.Value.Kind)
	}
}

func (c *QValueAvroConverter) processGoTime() (interface{}, error) {
	if c.Value.Value == nil && c.Nullable {
		return nil, nil
	}

	t, ok := c.Value.Value.(time.Time)
	if !ok {
		return nil, fmt.Errorf("invalid Time value")
	}

	ret := t.UnixMicro()
	// Snowflake has issues with avro timestamp types, returning as string form of the int64
	// See: https://stackoverflow.com/questions/66104762/snowflake-date-column-have-incorrect-date-from-avro-file
	if c.TargetDWH == QDWHTypeSnowflake {
		return fmt.Sprint(ret), nil
	}
	return ret, nil
}

func (c *QValueAvroConverter) processNullableUnion(
	avroType string,
	value interface{},
) (interface{}, error) {
	if value == nil && c.Nullable {
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
	if c.Value.Value == nil && c.Nullable {
		return nil, nil
	}

	byteData, ok := c.Value.Value.([]byte)
	if !ok {
		return nil, fmt.Errorf("invalid Bytes value")
	}

	if c.Nullable {
		return goavro.Union("bytes", byteData), nil
	}

	return byteData, nil
}

func (c *QValueAvroConverter) processJSON() (interface{}, error) {
	if c.Value.Value == nil && c.Nullable {
		return nil, nil
	}

	jsonString, ok := c.Value.Value.(string)
	if !ok {
		return nil, fmt.Errorf("invalid JSON value %v", c.Value.Value)
	}

	if c.Nullable {
		if c.TargetDWH == QDWHTypeSnowflake && len(jsonString) > 15*1024*1024 {
			log.Warn("Truncating JSON value > 15MB for Snowflake!")
			log.Warn("Check this issue for details: https://github.com/PeerDB-io/peerdb/issues/309")
			return goavro.Union("string", ""), nil
		}
		return goavro.Union("string", jsonString), nil
	}

	if c.TargetDWH == QDWHTypeSnowflake && len(jsonString) > 15*1024*1024 {
		log.Warn("Truncating JSON value > 15MB for Snowflake!")
		log.Warn("Check this issue for details: https://github.com/PeerDB-io/peerdb/issues/309")
		return "", nil
	}
	return jsonString, nil
}

func (c *QValueAvroConverter) processUUID() (interface{}, error) {
	if c.Value.Value == nil {
		return nil, nil
	}

	byteData, ok := c.Value.Value.([16]byte)
	if !ok {
		// attempt to convert google.uuid to [16]byte
		byteData, ok = c.Value.Value.(uuid.UUID)
		if !ok {
			return nil, fmt.Errorf("[conversion] invalid UUID value %v", c.Value.Value)
		}
	}

	u, err := uuid.FromBytes(byteData[:])
	if err != nil {
		return nil, fmt.Errorf("[conversion] conversion of invalid UUID value: %v", err)
	}

	uuidString := u.String()

	if c.Nullable {
		return goavro.Union("string", uuidString), nil
	}

	return uuidString, nil
}

func (c *QValueAvroConverter) processGeospatial() (interface{}, error) {
	if c.Value.Value == nil {
		return nil, nil
	}

	geoString, ok := c.Value.Value.(string)
	if !ok {
		return nil, fmt.Errorf("[conversion] invalid geospatial value %v", c.Value.Value)
	}

	return geoString, nil
}

func (c *QValueAvroConverter) processArrayInt32() (interface{}, error) {
	if c.Value.Value == nil && c.Nullable {
		return nil, nil
	}

	arrayData, ok := c.Value.Value.([]int32)
	if !ok {
		return nil, fmt.Errorf("invalid Int32 array value")
	}

	if c.Nullable {
		return goavro.Union("array", arrayData), nil
	}

	return arrayData, nil
}

func (c *QValueAvroConverter) processArrayInt64() (interface{}, error) {
	if c.Value.Value == nil && c.Nullable {
		return nil, nil
	}

	arrayData, ok := c.Value.Value.([]int64)
	if !ok {
		return nil, fmt.Errorf("invalid Int64 array value")
	}

	if c.Nullable {
		return goavro.Union("array", arrayData), nil
	}

	return arrayData, nil
}

func (c *QValueAvroConverter) processArrayFloat32() (interface{}, error) {
	if c.Value.Value == nil && c.Nullable {
		return nil, nil
	}

	arrayData, ok := c.Value.Value.([]float32)
	if !ok {
		return nil, fmt.Errorf("invalid Float32 array value")
	}

	if c.Nullable {
		return goavro.Union("array", arrayData), nil
	}

	return arrayData, nil
}

func (c *QValueAvroConverter) processArrayFloat64() (interface{}, error) {
	if c.Value.Value == nil && c.Nullable {
		return nil, nil
	}

	arrayData, ok := c.Value.Value.([]float64)
	if !ok {
		return nil, fmt.Errorf("invalid Float64 array value")
	}

	if c.Nullable {
		return goavro.Union("array", arrayData), nil
	}

	return arrayData, nil
}

func (c *QValueAvroConverter) processArrayString() (interface{}, error) {
	if c.Value.Value == nil && c.Nullable {
		return nil, nil
	}

	arrayData, ok := c.Value.Value.([]string)
	if !ok {
		return nil, fmt.Errorf("invalid String array value")
	}

	if c.Nullable {
		return goavro.Union("array", arrayData), nil
	}

	return arrayData, nil
}
