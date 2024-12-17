package qvalue

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/linkedin/goavro/v2"
	"github.com/shopspring/decimal"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peer-flow/datatypes"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

type AvroSchemaField struct {
	Name        string      `json:"name"`
	Type        interface{} `json:"type"`
	LogicalType string      `json:"logicalType,omitempty"`
}

type AvroSchemaLogical struct {
	Type        string `json:"type"`
	LogicalType string `json:"logicalType,omitempty"`
}

// https://avro.apache.org/docs/1.11.0/spec.html
// please make this generic at some point
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

func TruncateOrLogNumeric(num decimal.Decimal, precision int16, scale int16, targetDB protos.DBType) (decimal.Decimal, error) {
	if targetDB == protos.DBType_SNOWFLAKE || targetDB == protos.DBType_BIGQUERY {
		bidigi := datatypes.CountDigits(num.BigInt())
		avroPrecision, avroScale := DetermineNumericSettingForDWH(precision, scale, targetDB)
		if bidigi+int(avroScale) > int(avroPrecision) {
			slog.Warn("Clearing NUMERIC value with too many digits", slog.Any("number", num))
			return num, errors.New("invalid numeric")
		} else if num.Exponent() < -int32(avroScale) {
			num = num.Truncate(int32(avroScale))
			slog.Warn("Truncated NUMERIC value", slog.Any("number", num))
		}
	}
	return num, nil
}

// GetAvroSchemaFromQValueKind returns the Avro schema for a given QValueKind.
// The function takes in two parameters, a QValueKind and a boolean indicating if the
// Avro schema should respect null values. It returns a QValueKindAvroSchema object
// representing the Avro schema and an error if the QValueKind is unsupported.
//
// For example, QValueKindInt64 would return an AvroLogicalSchema of "long". Unsupported QValueKinds
// will return an error.
func GetAvroSchemaFromQValueKind(
	ctx context.Context,
	env map[string]string,
	kind QValueKind,
	targetDWH protos.DBType,
	precision int16,
	scale int16,
) (interface{}, error) {
	switch kind {
	case QValueKindString:
		return "string", nil
	case QValueKindQChar, QValueKindCIDR, QValueKindINET, QValueKindMacaddr:
		return "string", nil
	case QValueKindInterval:
		return "string", nil
	case QValueKindUUID:
		return AvroSchemaLogical{
			Type:        "string",
			LogicalType: "uuid",
		}, nil
	case QValueKindArrayUUID:
		return AvroSchemaComplexArray{
			Type: "array",
			Items: AvroSchemaField{
				Type:        "string",
				LogicalType: "uuid",
			},
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
	case QValueKindBytes:
		if targetDWH == protos.DBType_CLICKHOUSE {
			return "string", nil
		}
		return "bytes", nil
	case QValueKindNumeric:
		if targetDWH == protos.DBType_CLICKHOUSE {
			if precision == 0 && scale == 0 {
				asString, err := peerdbenv.PeerDBEnableClickHouseNumericAsString(ctx, env)
				if err != nil {
					return nil, err
				}
				if asString {
					return "string", nil
				}
			}
			if precision > datatypes.PeerDBClickHouseMaxPrecision {
				return "string", nil
			}
		}
		avroNumericPrecision, avroNumericScale := DetermineNumericSettingForDWH(precision, scale, targetDWH)
		return AvroSchemaNumeric{
			Type:        "bytes",
			LogicalType: "decimal",
			Precision:   avroNumericPrecision,
			Scale:       avroNumericScale,
		}, nil
	case QValueKindDate:
		if targetDWH == protos.DBType_CLICKHOUSE {
			return AvroSchemaLogical{
				Type:        "int",
				LogicalType: "date",
			}, nil
		}
		return "string", nil
	case QValueKindTime, QValueKindTimeTZ:
		if targetDWH == protos.DBType_CLICKHOUSE {
			return AvroSchemaLogical{
				Type:        "long",
				LogicalType: "time-micros",
			}, nil
		}
		return "string", nil
	case QValueKindTimestamp, QValueKindTimestampTZ:
		if targetDWH == protos.DBType_CLICKHOUSE {
			return AvroSchemaLogical{
				Type:        "long",
				LogicalType: "timestamp-micros",
			}, nil
		}
		return "string", nil
	case QValueKindTSTZRange:
		return "string", nil
	case QValueKindHStore, QValueKindJSON, QValueKindJSONB, QValueKindStruct:
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
		if targetDWH == protos.DBType_CLICKHOUSE {
			return AvroSchemaComplexArray{
				Type: "array",
				Items: AvroSchemaField{
					Type:        "int",
					LogicalType: "date",
				},
			}, nil
		}
		return AvroSchemaArray{
			Type:  "array",
			Items: "string",
		}, nil
	case QValueKindArrayTimestamp, QValueKindArrayTimestampTZ:
		if targetDWH == protos.DBType_CLICKHOUSE {
			return AvroSchemaComplexArray{
				Type: "array",
				Items: AvroSchemaField{
					Type:        "long",
					LogicalType: "timestamp-micros",
				},
			}, nil
		}
		return AvroSchemaArray{
			Type:  "array",
			Items: "string",
		}, nil
	case QValueKindArrayJSON, QValueKindArrayJSONB:
		return "string", nil
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
	*QField
	logger                   log.Logger
	TargetDWH                protos.DBType
	UnboundedNumericAsString bool
}

func QValueToAvro(
	value QValue, field *QField, targetDWH protos.DBType, logger log.Logger,
	unboundedNumericAsString bool,
) (any, error) {
	if value.Value() == nil {
		return nil, nil
	}

	c := QValueAvroConverter{
		QField:                   field,
		TargetDWH:                targetDWH,
		logger:                   logger,
		UnboundedNumericAsString: unboundedNumericAsString,
	}

	switch v := value.(type) {
	case QValueInvalid:
		// we will attempt to convert invalid to a string
		return c.processNullableUnion("string", v.Val)
	case QValueTime:
		t := c.processGoTime(v.Val)
		if t == nil {
			return nil, nil
		}
		if c.TargetDWH == protos.DBType_SNOWFLAKE {
			if c.Nullable {
				return c.processNullableUnion("string", t.(string))
			} else {
				return t.(string), nil
			}
		} else {
			if c.Nullable {
				return goavro.Union("long.time-micros", t.(int64)), nil
			}
			return t.(int64), nil
		}
	case QValueTimeTZ:
		t := c.processGoTimeTZ(v.Val)
		if t == nil {
			return nil, nil
		}
		if c.TargetDWH == protos.DBType_SNOWFLAKE {
			if c.Nullable {
				return c.processNullableUnion("string", t.(string))
			} else {
				return t.(string), nil
			}
		} else {
			if c.Nullable {
				return goavro.Union("long.time-micros", t.(int64)), nil
			}
			return t.(int64), nil
		}
	case QValueTimestamp:
		t := c.processGoTimestamp(v.Val)
		if t == nil {
			return nil, nil
		}
		if c.TargetDWH == protos.DBType_SNOWFLAKE {
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
		t := c.processGoTimestampTZ(v.Val)
		if t == nil {
			return nil, nil
		}
		if c.TargetDWH == protos.DBType_SNOWFLAKE {
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
		t := c.processGoDate(v.Val)
		if t == nil {
			return nil, nil
		}

		if c.TargetDWH == protos.DBType_SNOWFLAKE {
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
		return c.processNullableUnion("string", string(v.Val))
	case QValueString, QValueCIDR, QValueINET, QValueMacaddr, QValueInterval, QValueTSTZRange:
		if c.TargetDWH == protos.DBType_SNOWFLAKE && v.Value() != nil &&
			(len(v.Value().(string)) > 15*1024*1024) {
			slog.Warn("Clearing TEXT value > 15MB for Snowflake!")
			slog.Warn("Check this issue for details: https://github.com/PeerDB-io/peerdb/issues/309")
			return c.processNullableUnion("string", "")
		}
		return c.processNullableUnion("string", v.Value())
	case QValueFloat32:
		if c.TargetDWH == protos.DBType_BIGQUERY {
			return c.processNullableUnion("double", float64(v.Val))
		}
		return c.processNullableUnion("float", v.Val)
	case QValueFloat64:
		return c.processNullableUnion("double", v.Val)
	case QValueInt16:
		return c.processNullableUnion("long", int32(v.Val))
	case QValueInt32, QValueInt64:
		return c.processNullableUnion("long", v.Value())
	case QValueBoolean:
		return c.processNullableUnion("boolean", v.Val)
	case QValueStruct:
		return nil, errors.New("QValueStruct not supported")
	case QValueNumeric:
		return c.processNumeric(v.Val), nil
	case QValueBytes:
		return c.processBytes(v.Val), nil
	case QValueJSON:
		return c.processJSON(v.Val), nil
	case QValueHStore:
		return c.processHStore(v.Val)
	case QValueArrayFloat32:
		return c.processArrayFloat32(v.Val), nil
	case QValueArrayFloat64:
		return c.processArrayFloat64(v.Val), nil
	case QValueArrayInt16:
		return c.processArrayInt16(v.Val), nil
	case QValueArrayInt32:
		return c.processArrayInt32(v.Val), nil
	case QValueArrayInt64:
		return c.processArrayInt64(v.Val), nil
	case QValueArrayString:
		return c.processArrayString(v.Val), nil
	case QValueArrayBoolean:
		return c.processArrayBoolean(v.Val), nil
	case QValueArrayTimestamp, QValueArrayTimestampTZ:
		return c.processArrayTime(v.Value().([]time.Time)), nil
	case QValueArrayDate:
		return c.processArrayDate(v.Val), nil
	case QValueUUID:
		return c.processUUID(v.Val), nil
	case QValueArrayUUID:
		return c.processArrayUUID(v.Val), nil
	case QValueGeography, QValueGeometry, QValuePoint:
		return c.processGeospatial(v.Value().(string)), nil
	default:
		return nil, fmt.Errorf("[toavro] unsupported %T", value)
	}
}

func (c *QValueAvroConverter) processGoTimeTZ(t time.Time) interface{} {
	// Snowflake has issues with avro timestamp types, returning as string form
	// See: https://stackoverflow.com/questions/66104762/snowflake-date-column-have-incorrect-date-from-avro-file
	if c.TargetDWH == protos.DBType_SNOWFLAKE {
		return t.Format("15:04:05.999999-0700")
	}
	return t.UnixMicro()
}

func (c *QValueAvroConverter) processGoTime(t time.Time) interface{} {
	// Snowflake has issues with avro timestamp types, returning as string form
	// See: https://stackoverflow.com/questions/66104762/snowflake-date-column-have-incorrect-date-from-avro-file
	if c.TargetDWH == protos.DBType_SNOWFLAKE {
		return t.Format("15:04:05.999999")
	}

	return t.UnixMicro()
}

func (c *QValueAvroConverter) processGoTimestampTZ(t time.Time) interface{} {
	// Snowflake has issues with avro timestamp types, returning as string form
	// See: https://stackoverflow.com/questions/66104762/snowflake-date-column-have-incorrect-date-from-avro-file
	if c.TargetDWH == protos.DBType_SNOWFLAKE {
		return t.Format("2006-01-02 15:04:05.999999-0700")
	}

	// Bigquery will not allow timestamp if it is less than 1AD and more than 9999AD
	// So make such timestamps null
	if DisallowedTimestamp(c.TargetDWH, t, c.logger) {
		return nil
	}

	return t.UnixMicro()
}

func (c *QValueAvroConverter) processGoTimestamp(t time.Time) interface{} {
	// Snowflake has issues with avro timestamp types, returning as string form
	// See: https://stackoverflow.com/questions/66104762/snowflake-date-column-have-incorrect-date-from-avro-file
	if c.TargetDWH == protos.DBType_SNOWFLAKE {
		return t.Format("2006-01-02 15:04:05.999999")
	}

	// Bigquery will not allow timestamp if it is less than 1AD and more than 9999AD
	// So make such timestamps null
	if DisallowedTimestamp(c.TargetDWH, t, c.logger) {
		return nil
	}

	return t.UnixMicro()
}

func (c *QValueAvroConverter) processGoDate(t time.Time) interface{} {
	// Bigquery will not allow Date if it is less than 1AD and more than 9999AD
	// So make such Dates null
	if DisallowedTimestamp(c.TargetDWH, t, c.logger) {
		return nil
	}

	// Snowflake has issues with avro timestamp types, returning as string form
	// See: https://stackoverflow.com/questions/66104762/snowflake-date-column-have-incorrect-date-from-avro-file
	if c.TargetDWH == protos.DBType_SNOWFLAKE {
		return t.Format("2006-01-02")
	}
	return t
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

func (c *QValueAvroConverter) processNumeric(num decimal.Decimal) any {
	if (c.UnboundedNumericAsString && c.Precision == 0 && c.Scale == 0) ||
		(c.TargetDWH == protos.DBType_CLICKHOUSE && c.Precision > datatypes.PeerDBClickHouseMaxPrecision) {
		numStr, _ := c.processNullableUnion("string", num.String())
		return numStr
	}

	num, err := TruncateOrLogNumeric(num, c.Precision, c.Scale, c.TargetDWH)
	if err != nil {
		return nil
	}

	rat := num.Rat()
	if c.Nullable {
		return goavro.Union("bytes.decimal", rat)
	}
	return rat
}

func (c *QValueAvroConverter) processBytes(byteData []byte) interface{} {
	if c.TargetDWH == protos.DBType_CLICKHOUSE {
		encoded := base64.StdEncoding.EncodeToString(byteData)
		if c.Nullable {
			return goavro.Union("string", encoded)
		}
		return encoded
	}
	if c.Nullable {
		return goavro.Union("bytes", byteData)
	}
	return byteData
}

func (c *QValueAvroConverter) processJSON(jsonString string) interface{} {
	if c.Nullable {
		if c.TargetDWH == protos.DBType_SNOWFLAKE && len(jsonString) > 15*1024*1024 {
			slog.Warn("Clearing JSON value > 15MB for Snowflake!")
			slog.Warn("Check this issue for details: https://github.com/PeerDB-io/peerdb/issues/309")
			return goavro.Union("string", "")
		}
		return goavro.Union("string", jsonString)
	}

	if c.TargetDWH == protos.DBType_SNOWFLAKE && len(jsonString) > 15*1024*1024 {
		slog.Warn("Clearing JSON value > 15MB for Snowflake!")
		slog.Warn("Check this issue for details: https://github.com/PeerDB-io/peerdb/issues/309")
		return ""
	}
	return jsonString
}

func (c *QValueAvroConverter) processArrayBoolean(arrayData []bool) interface{} {
	if c.Nullable {
		return goavro.Union("array", arrayData)
	}

	return arrayData
}

func (c *QValueAvroConverter) processArrayTime(arrayTime []time.Time) interface{} {
	transformedTimeArr := make([]interface{}, 0, len(arrayTime))
	for _, t := range arrayTime {
		// Snowflake has issues with avro timestamp types, returning as string form
		// See: https://stackoverflow.com/questions/66104762/snowflake-date-column-have-incorrect-date-from-avro-file
		if c.TargetDWH == protos.DBType_SNOWFLAKE {
			transformedTimeArr = append(transformedTimeArr, t.String())
		} else {
			transformedTimeArr = append(transformedTimeArr, t)
		}
	}

	if c.Nullable {
		return goavro.Union("array", transformedTimeArr)
	}

	return transformedTimeArr
}

func (c *QValueAvroConverter) processArrayDate(arrayDate []time.Time) interface{} {
	transformedTimeArr := make([]interface{}, 0, len(arrayDate))
	for _, t := range arrayDate {
		if c.TargetDWH == protos.DBType_SNOWFLAKE {
			transformedTimeArr = append(transformedTimeArr, t.Format("2006-01-02"))
		} else {
			transformedTimeArr = append(transformedTimeArr, t)
		}
	}

	if c.Nullable {
		return goavro.Union("array", transformedTimeArr)
	}

	return transformedTimeArr
}

func (c *QValueAvroConverter) processHStore(hstore string) (interface{}, error) {
	jsonString, err := datatypes.ParseHstore(hstore)
	if err != nil {
		return "", fmt.Errorf("cannot parse %s: %w", hstore, err)
	}

	if c.Nullable {
		if c.TargetDWH == protos.DBType_SNOWFLAKE && len(jsonString) > 15*1024*1024 {
			slog.Warn("Clearing HStore equivalent JSON value > 15MB for Snowflake!")
			slog.Warn("Check this issue for details: https://github.com/PeerDB-io/peerdb/issues/309")
			return goavro.Union("string", ""), nil
		}
		return goavro.Union("string", jsonString), nil
	}

	if c.TargetDWH == protos.DBType_SNOWFLAKE && len(jsonString) > 15*1024*1024 {
		slog.Warn("Clearing HStore equivalent JSON value > 15MB for Snowflake!")
		slog.Warn("Check this issue for details: https://github.com/PeerDB-io/peerdb/issues/309")
		return "", nil
	}
	return jsonString, nil
}

func (c *QValueAvroConverter) processUUID(byteData uuid.UUID) interface{} {
	uuidString := byteData.String()
	if c.Nullable {
		return goavro.Union("string", uuidString)
	}
	return uuidString
}

func (c *QValueAvroConverter) processArrayUUID(arrayData []uuid.UUID) interface{} {
	UUIDData := make([]string, 0, len(arrayData))
	for _, uuid := range arrayData {
		UUIDData = append(UUIDData, uuid.String())
	}

	if c.Nullable {
		return goavro.Union("array", UUIDData)
	}

	return UUIDData
}

func (c *QValueAvroConverter) processGeospatial(geoString string) interface{} {
	if c.Nullable {
		return goavro.Union("string", geoString)
	}
	return geoString
}

func (c *QValueAvroConverter) processArrayInt16(arrayData []int16) interface{} {
	// cast to int32
	int32Data := make([]int32, 0, len(arrayData))
	for _, v := range arrayData {
		int32Data = append(int32Data, int32(v))
	}

	if c.Nullable {
		return goavro.Union("array", int32Data)
	}

	return int32Data
}

func (c *QValueAvroConverter) processArrayInt32(arrayData []int32) interface{} {
	if c.Nullable {
		return goavro.Union("array", arrayData)
	}
	return arrayData
}

func (c *QValueAvroConverter) processArrayInt64(arrayData []int64) interface{} {
	if c.Nullable {
		return goavro.Union("array", arrayData)
	}
	return arrayData
}

func (c *QValueAvroConverter) processArrayFloat32(arrayData []float32) interface{} {
	if c.Nullable {
		return goavro.Union("array", arrayData)
	}
	return arrayData
}

func (c *QValueAvroConverter) processArrayFloat64(arrayData []float64) interface{} {
	if c.Nullable {
		return goavro.Union("array", arrayData)
	}
	return arrayData
}

func (c *QValueAvroConverter) processArrayString(arrayData []string) interface{} {
	if c.Nullable {
		return goavro.Union("array", arrayData)
	}
	return arrayData
}
