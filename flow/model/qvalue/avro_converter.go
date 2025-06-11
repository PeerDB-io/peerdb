package qvalue

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hamba/avro/v2"
	"github.com/shopspring/decimal"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
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
	kind types.QValueKind,
	targetDWH protos.DBType,
	precision int16,
	scale int16,
) (avro.Schema, error) {
	switch kind {
	case types.QValueKindString, types.QValueKindEnum, types.QValueKindQChar, types.QValueKindCIDR,
		types.QValueKindINET, types.QValueKindMacaddr:
		return avro.NewPrimitiveSchema(avro.String, nil), nil
	case types.QValueKindInterval:
		return avro.NewPrimitiveSchema(avro.String, nil), nil
	case types.QValueKindUUID:
		return avro.NewPrimitiveSchema(avro.String, avro.NewPrimitiveLogicalSchema(avro.UUID)), nil
	case types.QValueKindArrayUUID:
		return avro.NewArraySchema(avro.NewPrimitiveSchema(avro.String, avro.NewPrimitiveLogicalSchema(avro.UUID))), nil
	case types.QValueKindGeometry, types.QValueKindGeography, types.QValueKindPoint:
		return avro.NewPrimitiveSchema(avro.String, nil), nil
	case types.QValueKindInt8, types.QValueKindInt16, types.QValueKindInt32, types.QValueKindInt64,
		types.QValueKindUInt8, types.QValueKindUInt16, types.QValueKindUInt32, types.QValueKindUInt64:
		return avro.NewPrimitiveSchema(avro.Long, nil), nil
	case types.QValueKindFloat32:
		if targetDWH == protos.DBType_BIGQUERY {
			return avro.NewPrimitiveSchema(avro.Double, nil), nil
		}
		return avro.NewPrimitiveSchema(avro.Float, nil), nil
	case types.QValueKindFloat64:
		return avro.NewPrimitiveSchema(avro.Double, nil), nil
	case types.QValueKindBoolean:
		return avro.NewPrimitiveSchema(avro.Boolean, nil), nil
	case types.QValueKindBytes:
		format, err := internal.PeerDBBinaryFormat(ctx, env)
		if err != nil {
			return nil, err
		}
		if targetDWH == protos.DBType_CLICKHOUSE && format != internal.BinaryFormatRaw {
			return avro.NewPrimitiveSchema(avro.String, nil), nil
		}
		return avro.NewPrimitiveSchema(avro.Bytes, nil), nil
	case types.QValueKindNumeric:
		return getAvroNumericSchema(ctx, env, targetDWH, precision, scale)
	case types.QValueKindDate:
		if targetDWH == protos.DBType_SNOWFLAKE {
			return avro.NewPrimitiveSchema(avro.String, nil), nil
		}
		return avro.NewPrimitiveSchema(avro.Int, avro.NewPrimitiveLogicalSchema(avro.Date)), nil
	case types.QValueKindTime, types.QValueKindTimeTZ:
		if targetDWH == protos.DBType_SNOWFLAKE {
			return avro.NewPrimitiveSchema(avro.String, nil), nil
		}
		return avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.TimeMicros)), nil
	case types.QValueKindTimestamp, types.QValueKindTimestampTZ:
		if targetDWH == protos.DBType_SNOWFLAKE {
			return avro.NewPrimitiveSchema(avro.String, nil), nil
		}
		return avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.TimestampMicros)), nil
	case types.QValueKindTSTZRange:
		return avro.NewPrimitiveSchema(avro.String, nil), nil
	case types.QValueKindHStore, types.QValueKindJSON, types.QValueKindJSONB:
		return avro.NewPrimitiveSchema(avro.String, nil), nil
	case types.QValueKindArrayFloat32:
		return avro.NewArraySchema(avro.NewPrimitiveSchema(avro.Float, nil)), nil
	case types.QValueKindArrayFloat64:
		return avro.NewArraySchema(avro.NewPrimitiveSchema(avro.Double, nil)), nil
	case types.QValueKindArrayInt32, types.QValueKindArrayInt16:
		return avro.NewArraySchema(avro.NewPrimitiveSchema(avro.Int, nil)), nil
	case types.QValueKindArrayInt64:
		return avro.NewArraySchema(avro.NewPrimitiveSchema(avro.Long, nil)), nil
	case types.QValueKindArrayBoolean:
		return avro.NewArraySchema(avro.NewPrimitiveSchema(avro.Boolean, nil)), nil
	case types.QValueKindArrayDate:
		if targetDWH == protos.DBType_SNOWFLAKE {
			return avro.NewArraySchema(avro.NewPrimitiveSchema(avro.String, nil)), nil
		}
		return avro.NewArraySchema(avro.NewPrimitiveSchema(avro.Int, avro.NewPrimitiveLogicalSchema(avro.Date))), nil
	case types.QValueKindArrayTimestamp, types.QValueKindArrayTimestampTZ:
		if targetDWH == protos.DBType_SNOWFLAKE {
			return avro.NewArraySchema(avro.NewPrimitiveSchema(avro.String, nil)), nil
		}
		return avro.NewArraySchema(avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.TimestampMicros))), nil
	case types.QValueKindArrayJSON, types.QValueKindArrayJSONB:
		return avro.NewPrimitiveSchema(avro.String, nil), nil
	case types.QValueKindArrayString, types.QValueKindArrayEnum:
		return avro.NewArraySchema(avro.NewPrimitiveSchema(avro.String, nil)), nil
	case types.QValueKindArrayNumeric:
		numericSchema, err := getAvroNumericSchema(ctx, env, targetDWH, precision, scale)
		if err != nil {
			return nil, err
		}
		return avro.NewArraySchema(numericSchema), nil
	case types.QValueKindInvalid:
		// lets attempt to do invalid as a string
		return avro.NewPrimitiveSchema(avro.String, nil), nil
	default:
		return nil, fmt.Errorf("unsupported types.QValueKind type: %s", kind)
	}
}

func getAvroNumericSchema(
	ctx context.Context,
	env map[string]string,
	targetDWH protos.DBType,
	precision int16,
	scale int16,
) (avro.Schema, error) {
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
}

type QValueAvroConverter struct {
	*types.QField
	logger                   log.Logger
	TargetDWH                protos.DBType
	UnboundedNumericAsString bool
}

func QValueToAvro(
	ctx context.Context, env map[string]string,
	value types.QValue, field *types.QField, targetDWH protos.DBType, logger log.Logger,
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
	case types.QValueInvalid:
		// we will attempt to convert invalid to a string
		return c.processNullableUnion(v.Val)
	case types.QValueTime:
		return c.processNullableUnion(c.processGoTime(v.Val))
	case types.QValueTimeTZ:
		return c.processNullableUnion(c.processGoTime(v.Val))
	case types.QValueTimestamp:
		return c.processNullableUnion(c.processGoTimestamp(v.Val))
	case types.QValueTimestampTZ:
		return c.processNullableUnion(c.processGoTimestampTZ(v.Val))
	case types.QValueDate:
		return c.processNullableUnion(c.processGoDate(v.Val))
	case types.QValueQChar:
		return c.processNullableUnion(string(v.Val))
	case types.QValueString,
		types.QValueCIDR, types.QValueINET, types.QValueMacaddr,
		types.QValueInterval, types.QValueTSTZRange, types.QValueEnum,
		types.QValueGeography, types.QValueGeometry, types.QValuePoint:
		if c.TargetDWH == protos.DBType_SNOWFLAKE && v.Value() != nil &&
			(len(v.Value().(string)) > 15*1024*1024) {
			slog.Warn("Clearing TEXT value > 15MB for Snowflake!")
			slog.Warn("Check this issue for details: https://github.com/PeerDB-io/peerdb/issues/309")
			return nil, nil
		}
		return c.processNullableUnion(v.Value())
	case types.QValueFloat32:
		if c.TargetDWH == protos.DBType_BIGQUERY {
			return c.processNullableUnion(float64(v.Val))
		}
		return c.processNullableUnion(v.Val)
	case types.QValueFloat64:
		return c.processNullableUnion(v.Val)
	case types.QValueInt8:
		return c.processNullableUnion(int64(v.Val))
	case types.QValueInt16:
		return c.processNullableUnion(int64(v.Val))
	case types.QValueInt32:
		return c.processNullableUnion(int64(v.Val))
	case types.QValueInt64:
		return c.processNullableUnion(v.Val)
	case types.QValueUInt8:
		return c.processNullableUnion(int64(v.Val))
	case types.QValueUInt16:
		return c.processNullableUnion(int64(v.Val))
	case types.QValueUInt32:
		return c.processNullableUnion(int64(v.Val))
	case types.QValueUInt64:
		return c.processNullableUnion(int64(v.Val))
	case types.QValueBoolean:
		return c.processNullableUnion(v.Val)
	case types.QValueNumeric:
		return c.processNumeric(v.Val), nil
	case types.QValueBytes:
		format, err := internal.PeerDBBinaryFormat(ctx, env)
		if err != nil {
			return nil, err
		}
		return c.processBytes(v.Val, format), nil
	case types.QValueJSON:
		return c.processJSON(v.Val), nil
	case types.QValueHStore:
		return c.processHStore(v.Val)
	case types.QValueArrayFloat32:
		return c.processArrayFloat32(v.Val), nil
	case types.QValueArrayFloat64:
		return c.processArrayFloat64(v.Val), nil
	case types.QValueArrayInt16:
		return c.processArrayInt16(v.Val), nil
	case types.QValueArrayInt32:
		return c.processArrayInt32(v.Val), nil
	case types.QValueArrayInt64:
		return c.processArrayInt64(v.Val), nil
	case types.QValueArrayString:
		return c.processArrayString(v.Val), nil
	case types.QValueArrayEnum:
		return c.processArrayString(v.Val), nil
	case types.QValueArrayBoolean:
		return c.processArrayBoolean(v.Val), nil
	case types.QValueArrayTimestamp:
		return c.processArrayTime(v.Val), nil
	case types.QValueArrayTimestampTZ:
		return c.processArrayTime(v.Val), nil
	case types.QValueArrayDate:
		return c.processArrayDate(v.Val), nil
	case types.QValueUUID:
		return c.processUUID(v.Val), nil
	case types.QValueArrayUUID:
		return c.processArrayUUID(v.Val), nil
	case types.QValueArrayNumeric:
		return c.processArrayNumeric(v.Val), nil
	default:
		return nil, fmt.Errorf("[toavro] unsupported %T", value)
	}
}

func (c *QValueAvroConverter) processGoTime(t time.Duration) any {
	// Snowflake has issues with avro timestamp types, returning as string form
	// See: https://stackoverflow.com/questions/66104762/snowflake-date-column-have-incorrect-date-from-avro-file
	if c.TargetDWH == protos.DBType_SNOWFLAKE {
		t = max(min(t, 86399999999), 0)
		return time.Time{}.Add(t).Format("15:04:05.999999")
	} else if c.TargetDWH == protos.DBType_BIGQUERY {
		return max(min(t, 86399999999), 0)
	} else if c.TargetDWH == protos.DBType_POSTGRES {
		return max(min(t, 86400000000), 0)
	} else {
		return t
	}
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

func (c *QValueAvroConverter) processArrayNumeric(arrayNum []decimal.Decimal) any {
	if (c.UnboundedNumericAsString && c.Precision == 0 && c.Scale == 0) ||
		(c.TargetDWH == protos.DBType_CLICKHOUSE && c.Precision > datatypes.PeerDBClickHouseMaxPrecision) {
		transformedNumArr := make([]string, 0, len(arrayNum))
		for _, num := range arrayNum {
			transformedNumArr = append(transformedNumArr, num.String())
		}
		return transformedNumArr
	}

	transformedNumArr := make([]*big.Rat, 0, len(arrayNum))
	for _, num := range arrayNum {
		num, err := TruncateOrLogNumeric(num, c.Precision, c.Scale, c.TargetDWH)
		if err != nil {
			transformedNumArr = append(transformedNumArr, nil)
			continue
		}
		transformedNumArr = append(transformedNumArr, num.Rat())
	}
	return transformedNumArr
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
