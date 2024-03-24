package qvalue

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/linkedin/goavro/v2"
	"github.com/shopspring/decimal"
	"go.temporal.io/sdk/log"

	hstore_util "github.com/PeerDB-io/peer-flow/hstore"
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
	Precision   int16  `json:"precision"`
	Scale       int16  `json:"scale"`
}

type AvroSchemaRecord struct {
	Type   string            `json:"type"`
	Name   string            `json:"name"`
	Fields []AvroSchemaField `json:"fields"`
}

type AvroSchemaLogical struct {
	Type        string `json:"type"`
	LogicalType string `json:"logicalType,omitempty"`
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
func GetAvroSchemaFromQValueKind(kind QValueKind, targetDWH QDWHType, precision int16, scale int16) (interface{}, error) {
	switch kind {
	case QValueKindString:
		return "string", nil
	case QValueKindQChar, QValueKindCIDR, QValueKindINET:
		return "string", nil
	case QValueKindInterval:
		return "string", nil
	case QValueKindUUID:
		return AvroSchemaLogical{
			Type:        "string",
			LogicalType: "uuid",
		}, nil
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
		avroNumericPrecision, avroNumericScale := DetermineNumericSettingForDWH(precision, scale, targetDWH)
		return AvroSchemaNumeric{
			Type:        "bytes",
			LogicalType: "decimal",
			Precision:   avroNumericPrecision,
			Scale:       avroNumericScale,
		}, nil
	case QValueKindTime, QValueKindTimeTZ, QValueKindDate:
		if targetDWH == QDWHTypeClickhouse {
			if kind == QValueKindTime {
				return "string", nil
			}
			if kind == QValueKindDate {
				return AvroSchemaLogical{
					Type:        "int",
					LogicalType: "date",
				}, nil
			}
			return "long", nil
		}
		return "string", nil
	case QValueKindTimestamp, QValueKindTimestampTZ:
		if targetDWH == QDWHTypeClickhouse {
			return AvroSchemaLogical{
				Type:        "long",
				LogicalType: "timestamp-micros",
			}, nil
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
	QValue
	TargetDWH QDWHType
	Nullable  bool
	logger    log.Logger
}

func NewQValueAvroConverter(value QValue, targetDWH QDWHType, nullable bool, logger log.Logger) *QValueAvroConverter {
	return &QValueAvroConverter{
		QValue:    value,
		TargetDWH: targetDWH,
		Nullable:  nullable,
		logger:    logger,
	}
}

func (c *QValueAvroConverter) ToAvroValue() (interface{}, error) {
	if c.Nullable && c.Value() == nil {
		return nil, nil
	}

	switch v := c.QValue.(type) {
	case QValueInvalid:
		// we will attempt to convert invalid to a string
		return c.processNullableUnion("string", c.Value())
	case QValueTime:
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
				return c.processNullableUnion("string", t.(string))
			} else {
				return t.(string), nil
			}
		}
		if c.Nullable {
			return goavro.Union("long.time-micros", t.(int64)), nil
		}
		return t.(int64), nil
	case QValueTimeTZ:
		t, err := c.processGoTimeTZ()
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
			return goavro.Union("long.time-micros", t.(int64)), nil
		}
		return t.(int64), nil
	case QValueTimestamp:
		t, err := c.processGoTimestamp()
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
		}
		return t.(int64), nil
	case QValueTimestampTZ:
		t, err := c.processGoTimestampTZ()
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
		}
		return t.(int64), nil
	case QValueDate:
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
		}
		return t, nil
	case QValueQChar:
		return c.processNullableUnion("string", string(c.Value().(uint8)))
	case QValueString, QValueCIDR, QValueINET, QValueMacaddr, QValueInterval:
		if c.TargetDWH == QDWHTypeSnowflake && c.Value() != nil &&
			(len(c.Value().(string)) > 15*1024*1024) {
			slog.Warn("Truncating TEXT value > 15MB for Snowflake!")
			slog.Warn("Check this issue for details: https://github.com/PeerDB-io/peerdb/issues/309")
			return c.processNullableUnion("string", "")
		}
		return c.processNullableUnion("string", c.Value())
	case QValueFloat32:
		if c.TargetDWH == QDWHTypeBigQuery {
			return c.processNullableUnion("double", c.Value())
		}
		return c.processNullableUnion("float", c.Value())
	case QValueFloat64:
		if c.TargetDWH == QDWHTypeSnowflake || c.TargetDWH == QDWHTypeBigQuery {
			if f32Val, ok := c.Value().(float32); ok {
				return c.processNullableUnion("double", float64(f32Val))
			}
		}
		return c.processNullableUnion("double", c.Value())
	case QValueInt16:
		return c.processNullableUnion("long", int32(v.Val))
	case QValueInt32, QValueInt64:
		return c.processNullableUnion("long", c.Value())
	case QValueBoolean:
		return c.processNullableUnion("boolean", c.Value())
	case QValueStruct:
		return nil, errors.New("QValueStruct not supported")
	case QValueNumeric:
		return c.processNumeric()
	case QValueBytes, QValueBit:
		return c.processBytes()
	case QValueJSON:
		return c.processJSON()
	case QValueHStore:
		return c.processHStore()
	case QValueArrayFloat32:
		return c.processArrayFloat32()
	case QValueArrayFloat64:
		return c.processArrayFloat64()
	case QValueArrayInt16:
		return c.processArrayInt16()
	case QValueArrayInt32:
		return c.processArrayInt32()
	case QValueArrayInt64:
		return c.processArrayInt64()
	case QValueArrayString:
		return c.processArrayString()
	case QValueArrayBoolean:
		return c.processArrayBoolean()
	case QValueArrayTimestamp, QValueArrayTimestampTZ:
		arrayTime, err := c.processArrayTime()
		if err != nil || arrayTime == nil {
			return arrayTime, err
		}

		return arrayTime, nil
	case QValueArrayDate:
		arrayDate, err := c.processArrayDate()
		if err != nil || arrayDate == nil {
			return arrayDate, err
		}

		return arrayDate, nil
	case QValueUUID:
		return c.processUUID()
	case QValueGeography, QValueGeometry, QValuePoint:
		return c.processGeospatial()
	default:
		return nil, fmt.Errorf("[toavro] unsupported %T", c.QValue)
	}
}

func (c *QValueAvroConverter) processGoTimeTZ() (interface{}, error) {
	if c.Value() == nil && c.Nullable {
		return nil, nil
	}

	t, ok := c.Value().(time.Time)
	if !ok {
		return nil, errors.New("invalid TimeTZ value")
	}

	// Snowflake has issues with avro timestamp types, returning as string form
	// See: https://stackoverflow.com/questions/66104762/snowflake-date-column-have-incorrect-date-from-avro-file
	if c.TargetDWH == QDWHTypeSnowflake {
		return t.Format("15:04:05.999999-0700"), nil
	}
	return t.UnixMicro(), nil
}

func (c *QValueAvroConverter) processGoTime() (interface{}, error) {
	if c.Value() == nil && c.Nullable {
		return nil, nil
	}

	t, ok := c.Value().(time.Time)
	if !ok {
		return nil, errors.New("invalid Time value")
	}

	// Snowflake has issues with avro timestamp types, returning as string form
	// See: https://stackoverflow.com/questions/66104762/snowflake-date-column-have-incorrect-date-from-avro-file
	if c.TargetDWH == QDWHTypeSnowflake {
		return t.Format("15:04:05.999999"), nil
	}
	if c.TargetDWH == QDWHTypeClickhouse {
		return t.Format("15:04:05.999999"), nil
	}

	return t.UnixMicro(), nil
}

func (c *QValueAvroConverter) processGoTimestampTZ() (interface{}, error) {
	if c.Value() == nil && c.Nullable {
		return nil, nil
	}

	t, ok := c.Value().(time.Time)
	if !ok {
		return nil, errors.New("invalid TimestampTZ value")
	}

	// Snowflake has issues with avro timestamp types, returning as string form
	// See: https://stackoverflow.com/questions/66104762/snowflake-date-column-have-incorrect-date-from-avro-file
	if c.TargetDWH == QDWHTypeSnowflake {
		return t.Format("2006-01-02 15:04:05.999999-0700"), nil
	}

	// Bigquery will not allow timestamp if it is less than 1AD and more than 9999AD
	// So make such timestamps null
	if DisallowedTimestamp(c.TargetDWH, t, c.logger) {
		return nil, nil
	}

	return t.UnixMicro(), nil
}

func (c *QValueAvroConverter) processGoTimestamp() (interface{}, error) {
	if c.Value() == nil && c.Nullable {
		return nil, nil
	}

	t, ok := c.Value().(time.Time)
	if !ok {
		return nil, errors.New("invalid Timestamp value")
	}

	// Snowflake has issues with avro timestamp types, returning as string form
	// See: https://stackoverflow.com/questions/66104762/snowflake-date-column-have-incorrect-date-from-avro-file
	if c.TargetDWH == QDWHTypeSnowflake {
		return t.Format("2006-01-02 15:04:05.999999"), nil
	}

	// Bigquery will not allow timestamp if it is less than 1AD and more than 9999AD
	// So make such timestamps null
	if DisallowedTimestamp(c.TargetDWH, t, c.logger) {
		return nil, nil
	}

	return t.UnixMicro(), nil
}

func (c *QValueAvroConverter) processGoDate() (interface{}, error) {
	if c.Value() == nil && c.Nullable {
		return nil, nil
	}

	t, ok := c.Value().(time.Time)
	if !ok {
		return nil, errors.New("invalid Time value for Date")
	}

	// Bigquery will not allow Date if it is less than 1AD and more than 9999AD
	// So make such Dates null
	if DisallowedTimestamp(c.TargetDWH, t, c.logger) {
		return nil, nil
	}

	// Snowflake has issues with avro timestamp types, returning as string form
	// See: https://stackoverflow.com/questions/66104762/snowflake-date-column-have-incorrect-date-from-avro-file
	if c.TargetDWH == QDWHTypeSnowflake {
		return t.Format("2006-01-02"), nil
	}
	return t, nil
}

func (c *QValueAvroConverter) processNullableUnion(
	avroType string,
	value interface{},
) (interface{}, error) {
	if c.Nullable {
		if value == nil {
			return nil, nil
		}
		return goavro.Union(avroType, value), nil
	}
	return value, nil
}

func (c *QValueAvroConverter) processNumeric() (interface{}, error) {
	if c.Value() == nil {
		return nil, nil
	}

	num, ok := c.Value().(decimal.Decimal)
	if !ok {
		return nil, fmt.Errorf("invalid Numeric value: expected decimal.Decimal, got %T", c.Value())
	}
	rat := num.Rat()

	if c.Nullable {
		return goavro.Union("bytes.decimal", rat), nil
	}

	return rat, nil
}

func (c *QValueAvroConverter) processBytes() (interface{}, error) {
	if c.Value() == nil && c.Nullable {
		return nil, nil
	}

	if c.TargetDWH == QDWHTypeClickhouse {
		bigNum, ok := c.Value().(decimal.Decimal)
		if !ok {
			return nil, fmt.Errorf("invalid Numeric value: expected decimal, got %T", c.Value())
		}
		num, ok := bigNum.Float64()
		if !ok {
			return nil, fmt.Errorf("not able to convert bigNum to float64 %+v", bigNum)
		}
		return goavro.Union("double", num), nil
	}

	byteData, ok := c.Value().([]byte)
	if !ok {
		return nil, errors.New("invalid Bytes value")
	}

	if c.Nullable {
		return goavro.Union("bytes", byteData), nil
	}

	return byteData, nil
}

func (c *QValueAvroConverter) processJSON() (interface{}, error) {
	if c.Value() == nil && c.Nullable {
		return nil, nil
	}

	jsonString, ok := c.Value().(string)
	if !ok {
		return nil, fmt.Errorf("invalid JSON value %v", c.Value())
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
	if c.Value() == nil && c.Nullable {
		return nil, nil
	}

	arrayData, ok := c.Value().([]bool)
	if !ok {
		return nil, errors.New("invalid Boolean array value")
	}

	if c.Nullable {
		return goavro.Union("array", arrayData), nil
	}

	return arrayData, nil
}

func (c *QValueAvroConverter) processArrayTime() (interface{}, error) {
	if c.Value() == nil && c.Nullable {
		return nil, nil
	}

	arrayTime, ok := c.Value().([]time.Time)
	if !ok {
		return nil, errors.New("invalid Timestamp array value")
	}

	transformedTimeArr := make([]interface{}, 0, len(arrayTime))
	for _, t := range arrayTime {
		// Snowflake has issues with avro timestamp types, returning as string form
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
	if c.Value() == nil && c.Nullable {
		return nil, nil
	}

	arrayDate, ok := c.Value().([]time.Time)
	if !ok {
		return nil, errors.New("invalid Date array value")
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
	if c.Value() == nil && c.Nullable {
		return nil, nil
	}

	hstoreString, ok := c.Value().(string)
	if !ok {
		return nil, fmt.Errorf("invalid HSTORE value %v", c.Value())
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
	if c.Value() == nil {
		return nil, nil
	}

	byteData, ok := c.Value().([16]byte)
	if !ok {
		// attempt to convert google.uuid to [16]byte
		byteData, ok = c.Value().(uuid.UUID)
		if !ok {
			return nil, fmt.Errorf("[conversion] invalid UUID value %v", c.Value())
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
	if c.Value() == nil {
		return nil, nil
	}

	geoString, ok := c.Value().(string)
	if !ok {
		return nil, fmt.Errorf("[conversion] invalid geospatial value %v", c.Value())
	}

	if c.Nullable {
		return goavro.Union("string", geoString), nil
	}
	return geoString, nil
}

func (c *QValueAvroConverter) processArrayInt16() (interface{}, error) {
	if c.Value() == nil && c.Nullable {
		return nil, nil
	}

	arrayData, ok := c.Value().([]int16)
	if !ok {
		return nil, errors.New("invalid Int16 array value")
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
	if c.Value() == nil && c.Nullable {
		return nil, nil
	}

	arrayData, ok := c.Value().([]int32)
	if !ok {
		return nil, errors.New("invalid Int32 array value")
	}

	if c.Nullable {
		return goavro.Union("array", arrayData), nil
	}

	return arrayData, nil
}

func (c *QValueAvroConverter) processArrayInt64() (interface{}, error) {
	if c.Value() == nil && c.Nullable {
		return nil, nil
	}

	arrayData, ok := c.Value().([]int64)
	if !ok {
		return nil, errors.New("invalid Int64 array value")
	}

	if c.Nullable {
		return goavro.Union("array", arrayData), nil
	}

	return arrayData, nil
}

func (c *QValueAvroConverter) processArrayFloat32() (interface{}, error) {
	if c.Value() == nil && c.Nullable {
		return nil, nil
	}

	arrayData, ok := c.Value().([]float32)
	if !ok {
		return nil, errors.New("invalid Float32 array value")
	}

	if c.Nullable {
		return goavro.Union("array", arrayData), nil
	}

	return arrayData, nil
}

func (c *QValueAvroConverter) processArrayFloat64() (interface{}, error) {
	if c.Value() == nil && c.Nullable {
		return nil, nil
	}

	arrayData, ok := c.Value().([]float64)
	if !ok {
		return nil, errors.New("invalid Float64 array value")
	}

	if c.Nullable {
		return goavro.Union("array", arrayData), nil
	}

	return arrayData, nil
}

func (c *QValueAvroConverter) processArrayString() (interface{}, error) {
	if c.Value() == nil && c.Nullable {
		return nil, nil
	}

	arrayData, ok := c.Value().([]string)
	if !ok {
		return nil, errors.New("invalid String array value")
	}

	if c.Nullable {
		return goavro.Union("array", arrayData), nil
	}

	return arrayData, nil
}
