package qvalue

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hamba/avro/v2"
	"github.com/shopspring/decimal"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/datatypes"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared/qvalue"
)

var re = regexp.MustCompile(`[^A-Za-z0-9_]`)

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

// ConvertToAvroCompatibleName converts a column name to a field name that is compatible with Avro.
func ConvertToAvroCompatibleName(columnName string) string {
	// Avro field names must:
	// start with [A-Za-z_]
	// subsequently contain only [A-Za-z0-9_]
	if columnName == "" {
		return "_"
	}
	// Ensure the first character is a letter or underscore
	if columnName[0] >= '0' && columnName[0] <= '9' {
		columnName = "_" + columnName
	}

	// Replace invalid characters with _
	return re.ReplaceAllString(columnName, "_")
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
	kind qvalue.QValueKind,
	targetDWH protos.DBType,
	precision int16,
	scale int16,
) (avro.Schema, error) {
	switch kind {
	case qvalue.QValueKindString, qvalue.QValueKindEnum, qvalue.QValueKindQChar, qvalue.QValueKindCIDR,
		qvalue.QValueKindINET, qvalue.QValueKindMacaddr:
		return avro.NewPrimitiveSchema(avro.String, nil), nil
	case qvalue.QValueKindInterval:
		return avro.NewPrimitiveSchema(avro.String, nil), nil
	case qvalue.QValueKindUUID:
		return avro.NewPrimitiveSchema(avro.String, avro.NewPrimitiveLogicalSchema(avro.UUID)), nil
	case qvalue.QValueKindArrayUUID:
		return avro.NewArraySchema(avro.NewPrimitiveSchema(avro.String, avro.NewPrimitiveLogicalSchema(avro.UUID))), nil
	case qvalue.QValueKindGeometry, qvalue.QValueKindGeography, qvalue.QValueKindPoint:
		return avro.NewPrimitiveSchema(avro.String, nil), nil
	case qvalue.QValueKindInt8, qvalue.QValueKindInt16, qvalue.QValueKindInt32, qvalue.QValueKindInt64,
		qvalue.QValueKindUInt8, qvalue.QValueKindUInt16, qvalue.QValueKindUInt32, qvalue.QValueKindUInt64:
		return avro.NewPrimitiveSchema(avro.Long, nil), nil
	case qvalue.QValueKindFloat32:
		if targetDWH == protos.DBType_BIGQUERY {
			return avro.NewPrimitiveSchema(avro.Double, nil), nil
		}
		return avro.NewPrimitiveSchema(avro.Float, nil), nil
	case qvalue.QValueKindFloat64:
		return avro.NewPrimitiveSchema(avro.Double, nil), nil
	case qvalue.QValueKindBoolean:
		return avro.NewPrimitiveSchema(avro.Boolean, nil), nil
	case qvalue.QValueKindBytes:
		format, err := internal.PeerDBBinaryFormat(ctx, env)
		if err != nil {
			return nil, err
		}
		if targetDWH == protos.DBType_CLICKHOUSE && format != internal.BinaryFormatRaw {
			return avro.NewPrimitiveSchema(avro.String, nil), nil
		}
		return avro.NewPrimitiveSchema(avro.Bytes, nil), nil
	case qvalue.QValueKindNumeric:
		if targetDWH == protos.DBType_CLICKHOUSE {
			if precision == 0 && scale == 0 {
				asString, err := internal.PeerDBEnableClickHouseNumericAsString(ctx, env)
				if err != nil {
					return nil, err
				}
				if asString {
					return avro.NewPrimitiveSchema(avro.String, nil), nil
				}
			}
			if precision > datatypes.PeerDBClickHouseMaxPrecision {
				return avro.NewPrimitiveSchema(avro.String, nil), nil
			}
		}
		avroNumericPrecision, avroNumericScale := DetermineNumericSettingForDWH(precision, scale, targetDWH)
		return avro.NewPrimitiveSchema(avro.Bytes, avro.NewDecimalLogicalSchema(int(avroNumericPrecision), int(avroNumericScale))), nil
	case qvalue.QValueKindDate:
		if targetDWH == protos.DBType_SNOWFLAKE {
			return avro.NewPrimitiveSchema(avro.String, nil), nil
		}
		return avro.NewPrimitiveSchema(avro.Int, avro.NewPrimitiveLogicalSchema(avro.Date)), nil
	case qvalue.QValueKindTime, qvalue.QValueKindTimeTZ:
		if targetDWH == protos.DBType_SNOWFLAKE {
			return avro.NewPrimitiveSchema(avro.String, nil), nil
		}
		return avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.TimeMicros)), nil
	case qvalue.QValueKindTimestamp, qvalue.QValueKindTimestampTZ:
		if targetDWH == protos.DBType_SNOWFLAKE {
			return avro.NewPrimitiveSchema(avro.String, nil), nil
		}
		return avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.TimestampMicros)), nil
	case qvalue.QValueKindTSTZRange:
		return avro.NewPrimitiveSchema(avro.String, nil), nil
	case qvalue.QValueKindHStore, qvalue.QValueKindJSON, qvalue.QValueKindJSONB:
		return avro.NewPrimitiveSchema(avro.String, nil), nil
	case qvalue.QValueKindArrayFloat32:
		return avro.NewArraySchema(avro.NewPrimitiveSchema(avro.Float, nil)), nil
	case qvalue.QValueKindArrayFloat64:
		return avro.NewArraySchema(avro.NewPrimitiveSchema(avro.Double, nil)), nil
	case qvalue.QValueKindArrayInt32, qvalue.QValueKindArrayInt16:
		return avro.NewArraySchema(avro.NewPrimitiveSchema(avro.Int, nil)), nil
	case qvalue.QValueKindArrayInt64:
		return avro.NewArraySchema(avro.NewPrimitiveSchema(avro.Long, nil)), nil
	case qvalue.QValueKindArrayBoolean:
		return avro.NewArraySchema(avro.NewPrimitiveSchema(avro.Boolean, nil)), nil
	case qvalue.QValueKindArrayDate:
		if targetDWH == protos.DBType_SNOWFLAKE {
			return avro.NewArraySchema(avro.NewPrimitiveSchema(avro.String, nil)), nil
		}
		return avro.NewArraySchema(avro.NewPrimitiveSchema(avro.Int, avro.NewPrimitiveLogicalSchema(avro.Date))), nil
	case qvalue.QValueKindArrayTimestamp, qvalue.QValueKindArrayTimestampTZ:
		if targetDWH == protos.DBType_SNOWFLAKE {
			return avro.NewArraySchema(avro.NewPrimitiveSchema(avro.String, nil)), nil
		}
		return avro.NewArraySchema(avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.TimestampMicros))), nil
	case qvalue.QValueKindArrayJSON, qvalue.QValueKindArrayJSONB:
		return avro.NewPrimitiveSchema(avro.String, nil), nil
	case qvalue.QValueKindArrayString, qvalue.QValueKindArrayEnum:
		return avro.NewArraySchema(avro.NewPrimitiveSchema(avro.String, nil)), nil
	case qvalue.QValueKindInvalid:
		// lets attempt to do invalid as a string
		return avro.NewPrimitiveSchema(avro.String, nil), nil
	default:
		return nil, fmt.Errorf("unsupported qvalue.QValueKind type: %s", kind)
	}
}

type QValueAvroConverter struct {
	*qvalue.QField
	logger                   log.Logger
	TargetDWH                protos.DBType
	UnboundedNumericAsString bool
}

func QValueToAvro(
	ctx context.Context, env map[string]string,
	value qvalue.QValue, field *qvalue.QField, targetDWH protos.DBType, logger log.Logger,
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
	case qvalue.QValueInvalid:
		// we will attempt to convert invalid to a string
		return c.processNullableUnion(v.Val)
	case qvalue.QValueTime:
		return c.processNullableUnion(c.processGoTime(v.Val))
	case qvalue.QValueTimeTZ:
		return c.processNullableUnion(c.processGoTimeTZ(v.Val))
	case qvalue.QValueTimestamp:
		return c.processNullableUnion(c.processGoTimestamp(v.Val))
	case qvalue.QValueTimestampTZ:
		return c.processNullableUnion(c.processGoTimestampTZ(v.Val))
	case qvalue.QValueDate:
		return c.processNullableUnion(c.processGoDate(v.Val))
	case qvalue.QValueQChar:
		return c.processNullableUnion(string(v.Val))
	case qvalue.QValueString,
		qvalue.QValueCIDR, qvalue.QValueINET, qvalue.QValueMacaddr,
		qvalue.QValueInterval, qvalue.QValueTSTZRange, qvalue.QValueEnum,
		qvalue.QValueGeography, qvalue.QValueGeometry, qvalue.QValuePoint:
		if c.TargetDWH == protos.DBType_SNOWFLAKE && v.Value() != nil &&
			(len(v.Value().(string)) > 15*1024*1024) {
			slog.Warn("Clearing TEXT value > 15MB for Snowflake!")
			slog.Warn("Check this issue for details: https://github.com/PeerDB-io/peerdb/issues/309")
			return nil, nil
		}
		return c.processNullableUnion(v.Value())
	case qvalue.QValueFloat32:
		if c.TargetDWH == protos.DBType_BIGQUERY {
			return c.processNullableUnion(float64(v.Val))
		}
		return c.processNullableUnion(v.Val)
	case qvalue.QValueFloat64:
		return c.processNullableUnion(v.Val)
	case qvalue.QValueInt8:
		return c.processNullableUnion(int64(v.Val))
	case qvalue.QValueInt16:
		return c.processNullableUnion(int64(v.Val))
	case qvalue.QValueInt32:
		return c.processNullableUnion(int64(v.Val))
	case qvalue.QValueInt64:
		return c.processNullableUnion(v.Val)
	case qvalue.QValueUInt8:
		return c.processNullableUnion(int64(v.Val))
	case qvalue.QValueUInt16:
		return c.processNullableUnion(int64(v.Val))
	case qvalue.QValueUInt32:
		return c.processNullableUnion(int64(v.Val))
	case qvalue.QValueUInt64:
		return c.processNullableUnion(int64(v.Val))
	case qvalue.QValueBoolean:
		return c.processNullableUnion(v.Val)
	case qvalue.QValueNumeric:
		return c.processNumeric(v.Val), nil
	case qvalue.QValueBytes:
		format, err := internal.PeerDBBinaryFormat(ctx, env)
		if err != nil {
			return nil, err
		}
		return c.processBytes(v.Val, format), nil
	case qvalue.QValueJSON:
		return c.processJSON(v.Val), nil
	case qvalue.QValueHStore:
		return c.processHStore(v.Val)
	case qvalue.QValueArrayFloat32:
		return c.processArrayFloat32(v.Val), nil
	case qvalue.QValueArrayFloat64:
		return c.processArrayFloat64(v.Val), nil
	case qvalue.QValueArrayInt16:
		return c.processArrayInt16(v.Val), nil
	case qvalue.QValueArrayInt32:
		return c.processArrayInt32(v.Val), nil
	case qvalue.QValueArrayInt64:
		return c.processArrayInt64(v.Val), nil
	case qvalue.QValueArrayString:
		return c.processArrayString(v.Val), nil
	case qvalue.QValueArrayEnum:
		return c.processArrayString(v.Val), nil
	case qvalue.QValueArrayBoolean:
		return c.processArrayBoolean(v.Val), nil
	case qvalue.QValueArrayTimestamp:
		return c.processArrayTime(v.Val), nil
	case qvalue.QValueArrayTimestampTZ:
		return c.processArrayTime(v.Val), nil
	case qvalue.QValueArrayDate:
		return c.processArrayDate(v.Val), nil
	case qvalue.QValueUUID:
		return c.processUUID(v.Val), nil
	case qvalue.QValueArrayUUID:
		return c.processArrayUUID(v.Val), nil
	default:
		return nil, fmt.Errorf("[toavro] unsupported %T", value)
	}
}

func (c *QValueAvroConverter) processGoTimeTZ(t time.Time) any {
	// Snowflake has issues with avro timestamp types, returning as string form
	// See: https://stackoverflow.com/questions/66104762/snowflake-date-column-have-incorrect-date-from-avro-file
	if c.TargetDWH == protos.DBType_SNOWFLAKE {
		return t.Format("15:04:05.999999-0700")
	}
	return time.Duration(t.UnixMicro()) * time.Microsecond
}

func (c *QValueAvroConverter) processGoTime(t time.Time) any {
	// Snowflake has issues with avro timestamp types, returning as string form
	// See: https://stackoverflow.com/questions/66104762/snowflake-date-column-have-incorrect-date-from-avro-file
	if c.TargetDWH == protos.DBType_SNOWFLAKE {
		return t.Format("15:04:05.999999")
	}
	return time.Duration(t.UnixMicro()) * time.Microsecond
}

func (c *QValueAvroConverter) processGoTimestampTZ(t time.Time) any {
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

	return t
}

func (c *QValueAvroConverter) processGoTimestamp(t time.Time) any {
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

	return t
}

func (c *QValueAvroConverter) processGoDate(t time.Time) any {
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
	value any,
) (any, error) {
	if c.Nullable {
		if value == nil {
			return nil, nil
		}
		return &value, nil
	}
	return value, nil
}

func (c *QValueAvroConverter) processNumeric(num decimal.Decimal) any {
	if (c.UnboundedNumericAsString && c.Precision == 0 && c.Scale == 0) ||
		(c.TargetDWH == protos.DBType_CLICKHOUSE && c.Precision > datatypes.PeerDBClickHouseMaxPrecision) {
		numStr, _ := c.processNullableUnion(num.String())
		return numStr
	}

	num, err := TruncateOrLogNumeric(num, c.Precision, c.Scale, c.TargetDWH)
	if err != nil {
		return nil
	}

	rat := num.Rat()
	if c.Nullable {
		return &rat
	}
	return rat
}

func (c *QValueAvroConverter) processBytes(byteData []byte, format internal.BinaryFormat) any {
	if c.TargetDWH == protos.DBType_CLICKHOUSE && format != internal.BinaryFormatRaw {
		var encoded string
		switch format {
		case internal.BinaryFormatBase64:
			encoded = base64.StdEncoding.EncodeToString(byteData)
		case internal.BinaryFormatHex:
			encoded = strings.ToUpper(hex.EncodeToString(byteData))
		default:
			panic(fmt.Sprintf("unhandled binary format: %d", format))
		}
		if c.Nullable {
			return &encoded
		}
		return encoded
	}
	if c.Nullable {
		return &byteData
	}
	return byteData
}

func (c *QValueAvroConverter) processJSON(jsonString string) any {
	if c.Nullable {
		if c.TargetDWH == protos.DBType_SNOWFLAKE && len(jsonString) > 15*1024*1024 {
			slog.Warn("Clearing JSON value > 15MB for Snowflake!")
			slog.Warn("Check this issue for details: https://github.com/PeerDB-io/peerdb/issues/309")
			return nil
		}
		return &jsonString
	}

	if c.TargetDWH == protos.DBType_SNOWFLAKE && len(jsonString) > 15*1024*1024 {
		slog.Warn("Clearing JSON value > 15MB for Snowflake!")
		slog.Warn("Check this issue for details: https://github.com/PeerDB-io/peerdb/issues/309")
		return ""
	}
	return jsonString
}

func (c *QValueAvroConverter) processArrayBoolean(arrayData []bool) any {
	return arrayData
}

func (c *QValueAvroConverter) processArrayTime(arrayTime []time.Time) any {
	if c.Nullable && arrayTime == nil {
		return nil
	}

	if c.TargetDWH == protos.DBType_SNOWFLAKE {
		// Snowflake has issues with avro timestamp types, returning as string form
		// See: https://stackoverflow.com/questions/66104762/snowflake-date-column-have-incorrect-date-from-avro-file
		transformedTimeArr := make([]string, 0, len(arrayTime))
		for _, t := range arrayTime {
			transformedTimeArr = append(transformedTimeArr, t.String())
		}
		return transformedTimeArr
	}

	return arrayTime
}

func (c *QValueAvroConverter) processArrayDate(arrayDate []time.Time) any {
	if c.Nullable && arrayDate == nil {
		return nil
	}

	if c.TargetDWH == protos.DBType_SNOWFLAKE {
		transformedTimeArr := make([]string, 0, len(arrayDate))
		for _, t := range arrayDate {
			transformedTimeArr = append(transformedTimeArr, t.Format("2006-01-02"))
		}
		return transformedTimeArr
	}

	return arrayDate
}

func (c *QValueAvroConverter) processHStore(hstore string) (any, error) {
	jsonString, err := datatypes.ParseHstore(hstore)
	if err != nil {
		return "", fmt.Errorf("cannot parse %s: %w", hstore, err)
	}

	if c.Nullable {
		if c.TargetDWH == protos.DBType_SNOWFLAKE && len(jsonString) > 15*1024*1024 {
			slog.Warn("Clearing HStore equivalent JSON value > 15MB for Snowflake!")
			slog.Warn("Check this issue for details: https://github.com/PeerDB-io/peerdb/issues/309")
			return nil, nil
		}
		return &jsonString, nil
	}

	if c.TargetDWH == protos.DBType_SNOWFLAKE && len(jsonString) > 15*1024*1024 {
		slog.Warn("Clearing HStore equivalent JSON value > 15MB for Snowflake!")
		slog.Warn("Check this issue for details: https://github.com/PeerDB-io/peerdb/issues/309")
		return "", nil
	}
	return jsonString, nil
}

func (c *QValueAvroConverter) processUUID(byteData uuid.UUID) any {
	uuidString := byteData.String()
	if c.Nullable {
		return &uuidString
	}
	return uuidString
}

func (c *QValueAvroConverter) processArrayUUID(arrayData []uuid.UUID) any {
	if c.Nullable && arrayData == nil {
		return nil
	}

	UUIDData := make([]string, 0, len(arrayData))
	for _, uuid := range arrayData {
		UUIDData = append(UUIDData, uuid.String())
	}
	return UUIDData
}

func (c *QValueAvroConverter) processArrayInt16(arrayData []int16) any {
	if c.Nullable && arrayData == nil {
		return nil
	}

	// cast to int32
	int32Data := make([]int32, 0, len(arrayData))
	for _, v := range arrayData {
		int32Data = append(int32Data, int32(v))
	}
	return int32Data
}

func (c *QValueAvroConverter) processArrayInt32(arrayData []int32) any {
	return arrayData
}

func (c *QValueAvroConverter) processArrayInt64(arrayData []int64) any {
	return arrayData
}

func (c *QValueAvroConverter) processArrayFloat32(arrayData []float32) any {
	return arrayData
}

func (c *QValueAvroConverter) processArrayFloat64(arrayData []float64) any {
	return arrayData
}

func (c *QValueAvroConverter) processArrayString(arrayData []string) any {
	return arrayData
}
