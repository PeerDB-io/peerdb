package qvalue

import (
	"fmt"
	"log/slog"
	"math/big"
	"time"

	hstore_util "github.com/PeerDB-io/peer-flow/hstore"
	"github.com/PeerDB-io/peer-flow/model/numeric"
	"github.com/google/uuid"
	"github.com/linkedin/goavro/v2"
)

// https://avro.apache.org/docs/1.11.0/spec.html
type AvroSchemaArray struct {
	Type  string `json:"type"`
	Items string `json:"items"`
}

type AvroSchemaComplexArray struct {
	Type  string          `json:"type"`
	Items AvroSchemaField `json:"items"`
}

type AvroSchemaNumeric struct {
	Type        string `json:"type"`
	LogicalType string `json:"logicalType"`
	Precision   int    `json:"precision"`
	Scale       int    `json:"scale"`
}

type AvroSchemaRecord struct {
	Type   string            `json:"type"`
	Name   string            `json:"name"`
	Fields []AvroSchemaField `json:"fields"`
}

type AvroSchemaField struct {
	Name        string      `json:"name"`
	Type        interface{} `json:"type"`
	LogicalType string      `json:"logicalType,omitempty"`
}

// GetAvroSchemaFromQValueKind returns the Avro schema for a given QValueKind.
// The function takes in two parameters, a QValueKind and a boolean indicating if the
// Avro schema should respect null values. It returns a QValueKindAvroSchema object
// representing the Avro schema and an error if the QValueKind is unsupported.
//
// For example, QValueKindInt64 would return an AvroLogicalSchema of "long". Unsupported QValueKinds
// will return an error.
func GetAvroSchemaFromQValueKind(kind QValueKind, targetDWH QDWHType) (interface{}, error) {
	switch kind {
	case QValueKindString, QValueKindUUID:
		return "string", nil
	case QValueKindGeometry, QValueKindGeography, QValueKindPoint:
		return "string", nil
	case QValueKindInt16, QValueKindInt32, QValueKindInt64:
		return "long", nil
	case QValueKindFloat32:
		return "float", nil
	case QValueKindFloat64:
		return "double", nil
	case QValueKindBoolean:
		return "boolean", nil
	case QValueKindBytes, QValueKindBit:
		return "bytes", nil
	case QValueKindNumeric:
		if targetDWH == QDWHTypeClickhouse {
			return "double", nil
		}
		return AvroSchemaNumeric{
			Type:        "bytes",
			LogicalType: "decimal",
			Precision:   38,
			Scale:       9,
		}, nil
	case QValueKindTime, QValueKindTimeTZ, QValueKindDate, QValueKindTimestamp, QValueKindTimestampTZ:
		if targetDWH == QDWHTypeClickhouse {
			return "long", nil
		}
		return "string", nil
	case QValueKindHStore, QValueKindJSON, QValueKindStruct:
		return "string", nil
	case QValueKindArrayFloat32:
		return AvroSchemaArray{
			Type:  "array",
			Items: "float",
		}, nil
	case QValueKindArrayFloat64:
		return AvroSchemaArray{
			Type:  "array",
			Items: "double",
		}, nil
	case QValueKindArrayInt32, QValueKindArrayInt16:
		return AvroSchemaArray{
			Type:  "array",
			Items: "int",
		}, nil
	case QValueKindArrayInt64:
		return AvroSchemaArray{
			Type:  "array",
			Items: "long",
		}, nil
	case QValueKindArrayBoolean:
		return AvroSchemaArray{
			Type:  "array",
			Items: "boolean",
		}, nil
	case QValueKindArrayDate:
		return AvroSchemaArray{
			Type:  "array",
			Items: "string",
		}, nil
	case QValueKindArrayTimestamp, QValueKindArrayTimestampTZ:
		return AvroSchemaArray{
			Type:  "array",
			Items: "string",
		}, nil
	case QValueKindArrayString:
		return AvroSchemaArray{
			Type:  "array",
			Items: "string",
		}, nil
	case QValueKindInvalid:
		// lets attempt to do invalid as a string
		return "string", nil
	default:
		return nil, fmt.Errorf("unsupported QValueKind type: %s", kind)
	}
}

type QValueAvroConverter struct {
	Value     QValue
	TargetDWH QDWHType
	Nullable  bool
}

func NewQValueAvroConverter(value QValue, targetDWH QDWHType, nullable bool) *QValueAvroConverter {
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
	case QValueKindTime, QValueKindTimeTZ, QValueKindTimestamp, QValueKindTimestampTZ:
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

		if c.TargetDWH == QDWHTypeClickhouse {
			if c.Nullable {
				return c.processNullableUnion("long", t.(int64))
			} else {
				return t.(int64), nil
			}
		}
		if c.Nullable {
			return goavro.Union("long.timestamp-micros", t.(int64)), nil
		}
		return t.(int64), nil

	case QValueKindDate:
		t, err := c.processGoDate()
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
			return goavro.Union("int.date", t), nil
		} else {
			return t, nil
		}

	case QValueKindString, QValueKindCIDR, QValueKindINET, QValueKindMacaddr:
		if c.TargetDWH == QDWHTypeSnowflake && c.Value.Value != nil &&
			(len(c.Value.Value.(string)) > 15*1024*1024) {
			slog.Warn("Truncating TEXT value > 15MB for Snowflake!")
			slog.Warn("Check this issue for details: https://github.com/PeerDB-io/peerdb/issues/309")
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
		return c.processHStore()
	case QValueKindArrayFloat32:
		return c.processArrayFloat32()
	case QValueKindArrayFloat64:
		return c.processArrayFloat64()
	case QValueKindArrayInt16:
		return c.processArrayInt16()
	case QValueKindArrayInt32:
		return c.processArrayInt32()
	case QValueKindArrayInt64:
		return c.processArrayInt64()
	case QValueKindArrayString:
		return c.processArrayString()
	case QValueKindArrayBoolean:
		return c.processArrayBoolean()
	case QValueKindArrayTimestamp, QValueKindArrayTimestampTZ:
		arrayTime, err := c.processArrayTime()
		if err != nil || arrayTime == nil {
			return arrayTime, err
		}

		return arrayTime, nil
	case QValueKindArrayDate:
		arrayDate, err := c.processArrayDate()
		if err != nil || arrayDate == nil {
			return arrayDate, err
		}

		return arrayDate, nil
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

func (c *QValueAvroConverter) processGoDate() (interface{}, error) {
	if c.Value.Value == nil && c.Nullable {
		return nil, nil
	}

	t, ok := c.Value.Value.(time.Time)
	if !ok {
		return nil, fmt.Errorf("invalid Time value for Date")
	}

	// Snowflake has issues with avro timestamp types, returning as string form of the int64
	// See: https://stackoverflow.com/questions/66104762/snowflake-date-column-have-incorrect-date-from-avro-file
	if c.TargetDWH == QDWHTypeSnowflake {
		ret := t.UnixMicro()
		return fmt.Sprint(ret), nil
	}
	return t, nil
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

	scale := numeric.PeerDBNumericScale
	decimalValue := num.FloatString(scale)
	num.SetString(decimalValue)
	if c.Nullable {
		return goavro.Union("bytes.decimal", num), nil
	}

	return num, nil
}

func (c *QValueAvroConverter) processBytes() (interface{}, error) {
	if c.Value.Value == nil && c.Nullable {
		return nil, nil
	}

	if c.TargetDWH == QDWHTypeClickhouse {
		bigNum, ok := c.Value.Value.(*big.Rat)
		if !ok {
			return nil, fmt.Errorf("invalid Numeric value: expected float64, got %T", c.Value.Value)
		}
		num, ok := bigNum.Float64()
		if !ok {
			return nil, fmt.Errorf("not able to convert bigNum to float64 %+v", bigNum)
		}
		return goavro.Union("double", num), nil
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
			slog.Warn("Truncating JSON value > 15MB for Snowflake!")
			slog.Warn("Check this issue for details: https://github.com/PeerDB-io/peerdb/issues/309")
			return goavro.Union("string", ""), nil
		}
		return goavro.Union("string", jsonString), nil
	}

	if c.TargetDWH == QDWHTypeSnowflake && len(jsonString) > 15*1024*1024 {
		slog.Warn("Truncating JSON value > 15MB for Snowflake!")
		slog.Warn("Check this issue for details: https://github.com/PeerDB-io/peerdb/issues/309")
		return "", nil
	}
	return jsonString, nil
}

func (c *QValueAvroConverter) processArrayBoolean() (interface{}, error) {
	if c.Value.Value == nil && c.Nullable {
		return nil, nil
	}

	arrayData, ok := c.Value.Value.([]bool)
	if !ok {
		return nil, fmt.Errorf("invalid Boolean array value")
	}

	if c.Nullable {
		return goavro.Union("array", arrayData), nil
	}

	return arrayData, nil
}

func (c *QValueAvroConverter) processArrayTime() (interface{}, error) {
	if c.Value.Value == nil && c.Nullable {
		return nil, nil
	}

	arrayTime, ok := c.Value.Value.([]time.Time)
	if !ok {
		return nil, fmt.Errorf("invalid Timestamp array value")
	}

	transformedTimeArr := make([]interface{}, 0, len(arrayTime))
	for _, t := range arrayTime {
		// Snowflake has issues with avro timestamp types, returning as string form of the int64
		// See: https://stackoverflow.com/questions/66104762/snowflake-date-column-have-incorrect-date-from-avro-file
		if c.TargetDWH == QDWHTypeSnowflake {
			transformedTimeArr = append(transformedTimeArr, t.String())
		} else {
			transformedTimeArr = append(transformedTimeArr, t)
		}
	}

	if c.Nullable {
		return goavro.Union("array", transformedTimeArr), nil
	}

	return transformedTimeArr, nil
}

func (c *QValueAvroConverter) processArrayDate() (interface{}, error) {
	if c.Value.Value == nil && c.Nullable {
		return nil, nil
	}

	arrayDate, ok := c.Value.Value.([]time.Time)
	if !ok {
		return nil, fmt.Errorf("invalid Date array value")
	}

	transformedTimeArr := make([]interface{}, 0, len(arrayDate))
	for _, t := range arrayDate {
		if c.TargetDWH == QDWHTypeSnowflake {
			transformedTimeArr = append(transformedTimeArr, t.Format("2006-01-02"))
		} else {
			transformedTimeArr = append(transformedTimeArr, t)
		}
	}

	if c.Nullable {
		return goavro.Union("array", transformedTimeArr), nil
	}

	return transformedTimeArr, nil
}

func (c *QValueAvroConverter) processHStore() (interface{}, error) {
	if c.Value.Value == nil && c.Nullable {
		return nil, nil
	}

	hstoreString, ok := c.Value.Value.(string)
	if !ok {
		return nil, fmt.Errorf("invalid HSTORE value %v", c.Value.Value)
	}

	jsonString, err := hstore_util.ParseHstore(hstoreString)
	if err != nil {
		return "", err
	}

	if c.Nullable {
		if c.TargetDWH == QDWHTypeSnowflake && len(jsonString) > 15*1024*1024 {
			slog.Warn("Truncating HStore equivalent JSON value > 15MB for Snowflake!")
			slog.Warn("Check this issue for details: https://github.com/PeerDB-io/peerdb/issues/309")
			return goavro.Union("string", ""), nil
		}
		return goavro.Union("string", jsonString), nil
	}

	if c.TargetDWH == QDWHTypeSnowflake && len(jsonString) > 15*1024*1024 {
		slog.Warn("Truncating HStore equivalent JSON value > 15MB for Snowflake!")
		slog.Warn("Check this issue for details: https://github.com/PeerDB-io/peerdb/issues/309")
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
		return nil, fmt.Errorf("[conversion] conversion of invalid UUID value: %w", err)
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

	if c.Nullable {
		return goavro.Union("string", geoString), nil
	}
	return geoString, nil
}

func (c *QValueAvroConverter) processArrayInt16() (interface{}, error) {
	if c.Value.Value == nil && c.Nullable {
		return nil, nil
	}

	arrayData, ok := c.Value.Value.([]int16)
	if !ok {
		return nil, fmt.Errorf("invalid Int16 array value")
	}

	// cast to int32
	int32Data := make([]int32, 0, len(arrayData))
	for _, v := range arrayData {
		int32Data = append(int32Data, int32(v))
	}

	if c.Nullable {
		return goavro.Union("array", int32Data), nil
	}

	return int32Data, nil
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
