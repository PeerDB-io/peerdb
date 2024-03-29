package qvalue

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"time"

	"github.com/google/uuid"
	"github.com/linkedin/goavro/v2"
	"github.com/shopspring/decimal"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	hstore_util "github.com/PeerDB-io/peer-flow/hstore"
)

// https://avro.apache.org/docs/1.11.0/spec.html
type AvroSchemaArray struct {
	Items interface{} `json:"items"`
	Type  string      `json:"type"`
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

// AvroSchema returns the Avro schema for a given QType.
//
// For example, QKindInt64 would return an AvroLogicalSchema of "long".
// Unsupported QValueKinds will return an error.
func (qt QType) AvroSchema(targetDWH protos.DBType, precision int16, scale int16) (interface{}, error) {
	if qt.Array > 0 {
		items, err := QType{Kind: qt.Kind, Array: qt.Array - 1}.AvroSchema(targetDWH, precision, scale)
		if err != nil {
			return nil, err
		}
		return AvroSchemaArray{
			Type:  "array",
			Items: items,
		}, nil
	}
	switch qt.Kind {
	case QKindString:
		return "string", nil
	case QKindQChar, QKindCIDR, QKindINET:
		return "string", nil
	case QKindInterval:
		return "string", nil
	case QKindUUID:
		return AvroSchemaLogical{
			Type:        "string",
			LogicalType: "uuid",
		}, nil
	case QKindGeometry, QKindGeography, QKindPoint:
		return "string", nil
	case QKindInt16, QKindInt32, QKindInt64:
		return "long", nil
	case QKindFloat32:
		return "float", nil
	case QKindFloat64:
		return "double", nil
	case QKindBoolean:
		return "boolean", nil
	case QKindBytes, QKindBit:
		return "bytes", nil
	case QKindNumeric:
		avroNumericPrecision, avroNumericScale := DetermineNumericSettingForDWH(precision, scale, targetDWH)
		return AvroSchemaNumeric{
			Type:        "bytes",
			LogicalType: "decimal",
			Precision:   avroNumericPrecision,
			Scale:       avroNumericScale,
		}, nil
	case QKindTime, QKindTimeTZ, QKindDate:
		if targetDWH == protos.DBType_CLICKHOUSE {
			if qt.Kind == QKindTime {
				return "string", nil
			}
			if qt.Kind == QKindDate {
				return AvroSchemaLogical{
					Type:        "int",
					LogicalType: "date",
				}, nil
			}
			return "long", nil
		}
		return "string", nil
	case QKindTimestamp, QKindTimestampTZ:
		if targetDWH == protos.DBType_CLICKHOUSE {
			return AvroSchemaLogical{
				Type:        "long",
				LogicalType: "timestamp-micros",
			}, nil
		}
		return "string", nil
	case QKindHStore, QKindJSON, QKindStruct:
		return "string", nil
	case QKindInvalid:
		// lets attempt to do invalid as a string
		return "string", nil
	default:
		return nil, fmt.Errorf("unsupported QType: %s", qt)
	}
}

type QValueAvroConverter struct {
	*QField
	logger    log.Logger
	TargetDWH protos.DBType
}

func QValueToAvro(value QValue, field *QField, targetDWH protos.DBType, logger log.Logger) (interface{}, error) {
	if value.Value() == nil {
		return nil, nil
	}

	c := &QValueAvroConverter{
		QField:    field,
		TargetDWH: targetDWH,
		logger:    logger,
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
		}

		if c.TargetDWH == protos.DBType_CLICKHOUSE {
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
		}

		if c.TargetDWH == protos.DBType_CLICKHOUSE {
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
	case QValueString, QValueCIDR, QValueINET, QValueMacaddr, QValueInterval:
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
	case QValueBit:
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
	case QValueArrayTimestamp:
		return c.processArrayTime(v.Val), nil
	case QValueArrayTimestampTZ:
		return c.processArrayTime(v.Val), nil
	case QValueArrayDate:
		return c.processArrayDate(v.Val), nil
	case QValueUUID:
		return c.processUUID(v.Val), nil
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
	if c.TargetDWH == protos.DBType_CLICKHOUSE {
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

var tenInt = big.NewInt(10)

func countDigits(bi *big.Int) int {
	if bi.IsUint64() {
		u64 := bi.Uint64()
		if u64 < (1 << 53) {
			if u64 == 0 {
				return 1
			}
			return int(math.Log10(float64(u64))) + 1
		}
	} else if bi.IsInt64() {
		i64 := bi.Int64()
		if i64 > -(1 << 53) {
			return int(math.Log10(float64(-i64))) + 1
		}
	}

	abs := new(big.Int).Abs(bi)
	// lg10 may be off by 1, need to verify
	lg10 := int(float64(abs.BitLen()) / math.Log2(10))
	check := big.NewInt(int64(lg10))
	return lg10 + abs.Cmp(check.Exp(tenInt, check, nil))
}

func (c *QValueAvroConverter) processNumeric(num decimal.Decimal) interface{} {
	if c.TargetDWH == protos.DBType_SNOWFLAKE {
		bidigi := countDigits(num.BigInt())
		avroPrecision, avroScale := DetermineNumericSettingForDWH(c.Precision, c.Scale, c.TargetDWH)

		if bidigi+int(avroScale) > int(avroPrecision) {
			slog.Warn("Clearing NUMERIC value with too many digits for Snowflake!", slog.Any("number", num))
			return nil
		} else if num.Exponent() < -int32(avroScale) {
			num = num.Round(int32(avroScale))
		}
	}
	rat := num.Rat()
	if c.Nullable {
		return goavro.Union("bytes.decimal", rat)
	}
	return rat
}

func (c *QValueAvroConverter) processBytes(byteData []byte) interface{} {
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
	jsonString, err := hstore_util.ParseHstore(hstore)
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

func (c *QValueAvroConverter) processUUID(byteData [16]byte) interface{} {
	uuidString := uuid.UUID(byteData).String()
	if c.Nullable {
		return goavro.Union("string", uuidString)
	}
	return uuidString
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
