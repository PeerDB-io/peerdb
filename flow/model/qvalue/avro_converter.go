package qvalue

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"math/bits"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hamba/avro/v2"
	"github.com/shopspring/decimal"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

var re = regexp.MustCompile(`[^A-Za-z0-9_]`)

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
	case types.QValueKindArrayInterval:
		return avro.NewArraySchema(avro.NewPrimitiveSchema(avro.String, nil)), nil
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
	case types.QValueKindInt256:
		return avro.NewFixedSchema("int256", "", 32, nil)
	case types.QValueKindUInt256:
		return avro.NewFixedSchema("uint256", "", 32, nil)
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

func NullableAvroSchema(schema avro.Schema) (avro.Schema, error) {
	return avro.NewUnionSchema([]avro.Schema{avro.NewNullSchema(), schema})
}

func getAvroNumericSchema(
	ctx context.Context,
	env map[string]string,
	targetDWH protos.DBType,
	precision int16,
	scale int16,
) (avro.Schema, error) {
	asString, err := internal.PeerDBEnableClickHouseNumericAsString(ctx, env)
	if err != nil {
		return nil, err
	}
	destinationType := GetNumericDestinationType(precision, scale, targetDWH, asString)
	if destinationType.IsString {
		return avro.NewPrimitiveSchema(avro.String, nil), nil
	}
	return avro.NewPrimitiveSchema(avro.Bytes,
		avro.NewDecimalLogicalSchema(int(destinationType.Precision), int(destinationType.Scale))), nil
}

type QValueAvroConverter struct {
	*types.QField
	logger                   log.Logger
	Stat                     *NumericStat
	TargetDWH                protos.DBType
	UnboundedNumericAsString bool
	binaryFormat             internal.BinaryFormat
}

type sizeOpt int8

const (
	sizeSkip sizeOpt = iota
	sizePlain
	sizeNullable
)

func (s sizeOpt) nullableSize() int64 {
	if s == sizeNullable {
		return 1
	}
	return 0
}

func QValueToAvro(
	ctx context.Context,
	value types.QValue, field *types.QField, targetDWH protos.DBType, logger log.Logger,
	unboundedNumericAsString bool, stat *NumericStat,
	binaryFormat internal.BinaryFormat,
	calcSize bool,
) (any, int64, error) {
	// Condense calcSize and nullable into a single value for shorter case statements
	sizeOpt := sizeSkip
	if calcSize {
		sizeOpt = sizePlain
		if field.Nullable {
			sizeOpt = sizeNullable
		}
	}

	if value.Value() == nil {
		return nil, sizeOpt.nullableSize(), nil
	}

	c := QValueAvroConverter{
		QField:                   field,
		logger:                   logger,
		Stat:                     stat,
		TargetDWH:                targetDWH,
		UnboundedNumericAsString: unboundedNumericAsString,
		binaryFormat:             binaryFormat,
	}

	switch v := value.(type) {
	case types.QValueInvalid:
		return c.processNullableUnion(v.Val), stringSize(v.Val, sizeOpt), nil
	case types.QValueTime:
		val, size := c.processGoTime(v.Val, sizeOpt)
		return c.processNullableUnion(val), size, nil
	case types.QValueTimeTZ:
		val, size := c.processGoTime(v.Val, sizeOpt)
		return c.processNullableUnion(val), size, nil
	case types.QValueTimestamp:
		val, size := c.processGoTimestamp(v.Val, sizeOpt)
		return c.processNullableUnion(val), size, nil
	case types.QValueTimestampTZ:
		val, size := c.processGoTimestampTZ(v.Val, sizeOpt)
		return c.processNullableUnion(val), size, nil
	case types.QValueDate:
		val, size := c.processGoDate(v.Val, sizeOpt)
		return c.processNullableUnion(val), size, nil
	case types.QValueQChar:
		return c.processNullableUnion(string(v.Val)), constSize(2, sizeOpt), nil
	case types.QValueString,
		types.QValueCIDR, types.QValueINET, types.QValueMacaddr,
		types.QValueInterval, types.QValueEnum,
		types.QValueGeography, types.QValueGeometry, types.QValuePoint:
		size := stringSize(v.Value().(string), sizeOpt)
		if c.TargetDWH == protos.DBType_SNOWFLAKE && v.Value() != nil &&
			(len(v.Value().(string)) > 15*1024*1024) {
			slog.WarnContext(ctx, "Clearing TEXT value > 15MB for Snowflake!")
			slog.WarnContext(ctx, "Check this issue for details: https://github.com/PeerDB-io/peerdb/issues/309")

			if c.Nullable {
				return nil, size, nil
			} else {
				return "", size, nil
			}
		}
		return c.processNullableUnion(v.Value()), size, nil
	case types.QValueFloat32:
		size := constSize(4, sizeOpt)
		if c.TargetDWH == protos.DBType_BIGQUERY {
			return c.processNullableUnion(float64(v.Val)), size, nil
		}
		return c.processNullableUnion(v.Val), size, nil
	case types.QValueFloat64:
		return c.processNullableUnion(v.Val), constSize(8, sizeOpt), nil
	case types.QValueInt8:
		return c.processNullableUnion(int64(v.Val)), varIntSize(int64(v.Val), sizeOpt), nil
	case types.QValueInt16:
		return c.processNullableUnion(int64(v.Val)), varIntSize(int64(v.Val), sizeOpt), nil
	case types.QValueInt32:
		return c.processNullableUnion(int64(v.Val)), varIntSize(int64(v.Val), sizeOpt), nil
	case types.QValueInt64:
		return c.processNullableUnion(v.Val), varIntSize(v.Val, sizeOpt), nil
	case types.QValueInt256:
		return c.processInt256(v.Val), constSize(32, sizeOpt), nil
	case types.QValueUInt8:
		return c.processNullableUnion(int64(v.Val)), varIntSize(int64(v.Val), sizeOpt), nil
	case types.QValueUInt16:
		return c.processNullableUnion(int64(v.Val)), varIntSize(int64(v.Val), sizeOpt), nil
	case types.QValueUInt32:
		return c.processNullableUnion(int64(v.Val)), varIntSize(int64(v.Val), sizeOpt), nil
	case types.QValueUInt64:
		return c.processNullableUnion(int64(v.Val)), varIntSize(int64(v.Val), sizeOpt), nil
	case types.QValueUInt256:
		return c.processUInt256(v.Val), constSize(32, sizeOpt), nil
	case types.QValueBoolean:
		return c.processNullableUnion(v.Val), constSize(1, sizeOpt), nil
	case types.QValueNumeric:
		val, size := c.processNumeric(v.Val, sizeOpt)
		return val, size, nil
	case types.QValueBytes:
		val, size := c.processBytes(v.Val, sizeOpt)
		return val, size, nil
	case types.QValueJSON:
		return c.processJSON(v.Val), stringSize(v.Val, sizeOpt), nil
	case types.QValueHStore:
		val, err := c.processHStore(v.Val)
		if err != nil {
			return nil, 0, fmt.Errorf("error processing HStore value: %w", err)
		}
		return val, stringSize(v.Val, sizeOpt), nil
	case types.QValueArrayFloat32:
		return c.processArrayFloat32(v.Val), fixedArraySize(len(v.Val), 4, sizeOpt), nil
	case types.QValueArrayFloat64:
		return c.processArrayFloat64(v.Val), fixedArraySize(len(v.Val), 8, sizeOpt), nil
	case types.QValueArrayInt16:
		return c.processArrayInt16(v.Val), varIntArraySize(v.Val, sizeOpt), nil
	case types.QValueArrayInt32:
		return c.processArrayInt32(v.Val), varIntArraySize(v.Val, sizeOpt), nil
	case types.QValueArrayInt64:
		return c.processArrayInt64(v.Val), varIntArraySize(v.Val, sizeOpt), nil
	case types.QValueArrayString:
		return c.processArrayString(v.Val), stringArraySize(v.Val, sizeOpt), nil
	case types.QValueArrayEnum:
		return c.processArrayString(v.Val), stringArraySize(v.Val, sizeOpt), nil
	case types.QValueArrayInterval:
		return c.processArrayString(v.Val), stringArraySize(v.Val, sizeOpt), nil
	case types.QValueArrayBoolean:
		return c.processArrayBoolean(v.Val), fixedArraySize(len(v.Val), 1, sizeOpt), nil
	case types.QValueArrayTimestamp:
		val, size := c.processArrayTime(v.Val, sizeOpt)
		return val, size, nil
	case types.QValueArrayTimestampTZ:
		val, size := c.processArrayTime(v.Val, sizeOpt)
		return val, size, nil
	case types.QValueArrayDate:
		val, size := c.processArrayDate(v.Val, sizeOpt)
		return val, size, nil
	case types.QValueUUID:
		val, size := c.processUUID(v.Val, sizeOpt)
		return val, size, nil
	case types.QValueArrayUUID:
		val, size := c.processArrayUUID(v.Val, sizeOpt)
		return val, size, nil
	case types.QValueArrayNumeric:
		val, size := c.processArrayNumeric(v.Val, sizeOpt)
		return val, size, nil
	default:
		return nil, 0, fmt.Errorf("[QValueToAvro] unsupported %T", value)
	}
}

func (c *QValueAvroConverter) processGoTime(t time.Duration, so sizeOpt) (any, int64) {
	// Snowflake has issues with avro timestamp types, returning as string form
	// See: https://stackoverflow.com/questions/66104762/snowflake-date-column-have-incorrect-date-from-avro-file
	if c.TargetDWH == protos.DBType_SNOWFLAKE {
		// Snowflake TIME must be in range [0, 24h)
		t = max(min(t, 86399999999*time.Microsecond), 0)
		s := time.Time{}.Add(t).Format("15:04:05.999999")
		return s, stringSize(s, so)
	}
	return t, constSize(8, so)
}

func (c *QValueAvroConverter) processGeneralTime(t time.Time, format string, avroVal int64, so sizeOpt) (any, int64) {
	// Bigquery will not allow timestamp if it is less than 1AD and more than 9999AD
	switch c.TargetDWH {
	case protos.DBType_BIGQUERY:
		year := t.Year()
		if year < 1 || year > 9999 {
			c.logger.Warn("Nulling Timestamp value for BigQuery as it exceeds allowed range",
				"timestamp", t.String())
			if c.Nullable {
				return nil, so.nullableSize()
			} else {
				return time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), varIntSize(0, so)
			}
		}
	case protos.DBType_SNOWFLAKE:
		// Snowflake has issues with avro timestamp types, returning as string form
		// See: https://stackoverflow.com/questions/66104762/snowflake-date-column-have-incorrect-date-from-avro-file
		s := t.Format(format)
		return s, stringSize(s, so)
	}
	return t, varIntSize(avroVal, so)
}

func (c *QValueAvroConverter) processGoTimestampTZ(t time.Time, so sizeOpt) (any, int64) {
	return c.processGeneralTime(t, "2006-01-02 15:04:05.999999-0700", t.UnixMicro(), so)
}

func (c *QValueAvroConverter) processGoTimestamp(t time.Time, so sizeOpt) (any, int64) {
	return c.processGeneralTime(t, "2006-01-02 15:04:05.999999", t.UnixMicro(), so)
}

func (c *QValueAvroConverter) processGoDate(t time.Time, so sizeOpt) (any, int64) {
	// Date is days since epoch, encoded as Avro int (varint)
	return c.processGeneralTime(t, "2006-01-02", t.Unix()/86400, so)
}

func (c *QValueAvroConverter) processNullableUnion(
	value any,
) any {
	if c.Nullable {
		if value == nil {
			return nil
		}
		return &value
	}
	return value
}

// Avro size of zero: varint(1) + 1 byte = 2
const zeroDecimalSize = 2

func (c *QValueAvroConverter) processNumeric(num decimal.Decimal, so sizeOpt) (any, int64) {
	destType := GetNumericDestinationType(c.Precision, c.Scale, c.TargetDWH, c.UnboundedNumericAsString)
	if destType.IsString {
		s := num.String()
		return c.processNullableUnion(s), stringSize(s, so)
	}

	num, intDigits, ok := TruncateNumeric(num, destType.Precision, destType.Scale, c.TargetDWH, c.Stat)
	if !ok {
		if c.Nullable {
			return nil, so.nullableSize()
		}
		return big.Rat{}, constSize(zeroDecimalSize, so)
	}

	return c.processNullableUnion(num.Rat()), decimalSize(num, intDigits, int(destType.Scale), so)
}

// decimalSize estimates Avro-encoded size from integer digit count and scale.
// Total digits = intDigits + scale determines byte length of two's complement encoding.
func decimalSize(num decimal.Decimal, intDigits, scale int, so sizeOpt) int64 {
	if so == sizeSkip {
		return 0
	}
	if num.IsZero() {
		return so.nullableSize() + zeroDecimalSize
	}

	// log2(10) gives exact bits needed for a power of 10, but actual values
	// may need fewer bits (e.g., 127 needs 7 bits, not 10 bits like 999). This can
	// overestimate by ~1 byte for values well below the max for their digit count.
	estimatedBitLen := max(int(float64(intDigits+scale)*math.Log2(10)), 8)
	byteLen := int64((estimatedBitLen + 9) / 8) // +9 for sign bit and rounding
	return so.nullableSize() + varIntSize(byteLen, sizePlain) + byteLen
}

var (
	//  2^256
	twoPow256 = new(big.Int).Lsh(big.NewInt(1), 256)
	//  maxInt256 =  2^255 − 1
	maxInt256 = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 255), big.NewInt(1))
	//  minInt256 = −2^255
	minInt256 = new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 255))
)

func bigIntTo32Bytes(n *big.Int) [32]uint8 {
	var out [32]uint8
	b := n.Bytes()
	for i := range b {
		out[i] = b[len(b)-1-i]
	}
	return out
}

func (c *QValueAvroConverter) processInt256(num *big.Int) any {
	if num.Cmp(minInt256) < 0 || num.Cmp(maxInt256) > 0 {
		c.Stat.BigInt256ClearedCount++
		if c.Nullable {
			return nil
		}
		return bigIntTo32Bytes(big.NewInt(0))
	}

	if num.Sign() < 0 {
		num = new(big.Int).Add(num, twoPow256)
	}

	return c.processNullableUnion(bigIntTo32Bytes(num))
}

func (c *QValueAvroConverter) processUInt256(num *big.Int) any {
	if num.Sign() < 0 || num.Cmp(twoPow256) >= 0 {
		c.Stat.BigInt256ClearedCount++
		if c.Nullable {
			return nil
		}
		return bigIntTo32Bytes(big.NewInt(0))
	}

	return c.processNullableUnion(bigIntTo32Bytes(num))
}

func (c *QValueAvroConverter) processArrayNumeric(arrayNum []decimal.Decimal, so sizeOpt) (any, int64) {
	destType := GetNumericDestinationType(c.Precision, c.Scale, c.TargetDWH, c.UnboundedNumericAsString)
	if destType.IsString {
		transformedNumArr := make([]string, 0, len(arrayNum))
		totalElemSize := int64(0)
		for _, num := range arrayNum {
			s := num.String()
			transformedNumArr = append(transformedNumArr, s)
			totalElemSize += stringSize(s, sizePlain)
		}
		return transformedNumArr, arraySize(len(arrayNum), totalElemSize, so)
	}

	transformedNumArr := make([]*big.Rat, 0, len(arrayNum))
	totalElemSize := int64(0)
	for _, num := range arrayNum {
		num, intDigits, ok := TruncateNumeric(num, destType.Precision, destType.Scale, c.TargetDWH, c.Stat)
		if !ok {
			transformedNumArr = append(transformedNumArr, &big.Rat{})
			totalElemSize += zeroDecimalSize
			continue
		}
		transformedNumArr = append(transformedNumArr, num.Rat())
		totalElemSize += decimalSize(num, intDigits, int(destType.Scale), sizePlain)
	}
	return transformedNumArr, arraySize(len(arrayNum), totalElemSize, so)
}

func (c *QValueAvroConverter) processBytes(byteData []byte, so sizeOpt) (any, int64) {
	if c.TargetDWH == protos.DBType_CLICKHOUSE && c.binaryFormat != internal.BinaryFormatRaw {
		var encoded string
		switch c.binaryFormat {
		case internal.BinaryFormatBase64:
			encoded = base64.StdEncoding.EncodeToString(byteData)
		case internal.BinaryFormatHex:
			encoded = strings.ToUpper(hex.EncodeToString(byteData))
		default:
			panic(fmt.Sprintf("unhandled binary format: %d", c.binaryFormat))
		}
		size := stringSize(encoded, so)
		if c.Nullable {
			return &encoded, size
		}
		return encoded, size
	}
	size := stringSize(shared.UnsafeFastReadOnlyBytesToString(byteData), so)
	if c.Nullable {
		return &byteData, size
	}
	return byteData, size
}

func (c *QValueAvroConverter) processJSON(jsonString string) any {
	if c.Nullable {
		if c.TargetDWH == protos.DBType_SNOWFLAKE && len(jsonString) > 15*1024*1024 {
			c.logger.Warn("Clearing JSON value > 15MB for Snowflake!")
			c.logger.Warn("Check this issue for details: https://github.com/PeerDB-io/peerdb/issues/309")
			return nil
		}
		return &jsonString
	}

	if c.TargetDWH == protos.DBType_SNOWFLAKE && len(jsonString) > 15*1024*1024 {
		c.logger.Warn("Clearing JSON value > 15MB for Snowflake!")
		c.logger.Warn("Check this issue for details: https://github.com/PeerDB-io/peerdb/issues/309")
		return ""
	}
	return jsonString
}

func (c *QValueAvroConverter) processArrayBoolean(arrayData []bool) any {
	return arrayData
}

func (c *QValueAvroConverter) processArrayTime(arrayTime []time.Time, so sizeOpt) (any, int64) {
	if c.Nullable && arrayTime == nil {
		return nil, so.nullableSize()
	}

	if c.TargetDWH == protos.DBType_SNOWFLAKE {
		// Snowflake has issues with avro timestamp types, returning as string form
		// See: https://stackoverflow.com/questions/66104762/snowflake-date-column-have-incorrect-date-from-avro-file
		transformedTimeArr := make([]string, 0, len(arrayTime))
		totalElemSize := int64(0)
		for _, t := range arrayTime {
			s := t.String()
			transformedTimeArr = append(transformedTimeArr, s)
			totalElemSize += stringSize(s, sizePlain)
		}
		return transformedTimeArr, arraySize(len(arrayTime), totalElemSize, so)
	}

	return arrayTime, fixedArraySize(len(arrayTime), 8, so)
}

func (c *QValueAvroConverter) processArrayDate(arrayDate []time.Time, so sizeOpt) (any, int64) {
	if c.Nullable && arrayDate == nil {
		return nil, so.nullableSize()
	}

	if c.TargetDWH == protos.DBType_SNOWFLAKE {
		transformedTimeArr := make([]string, 0, len(arrayDate))
		totalElemSize := int64(0)
		for _, t := range arrayDate {
			s := t.Format("2006-01-02")
			transformedTimeArr = append(transformedTimeArr, s)
			totalElemSize += stringSize(s, sizePlain)
		}
		return transformedTimeArr, arraySize(len(arrayDate), totalElemSize, so)
	}

	// Date is days since epoch, encoded as Avro int (varint)
	totalElemSize := int64(0)
	for _, t := range arrayDate {
		days := int32(t.Unix() / 86400)
		totalElemSize += varIntSize(int64(days), sizePlain)
	}
	return arrayDate, arraySize(len(arrayDate), totalElemSize, so)
}

func (c *QValueAvroConverter) processHStore(hstore string) (any, error) {
	jsonString, err := datatypes.ParseHstore(hstore)
	if err != nil {
		return "", fmt.Errorf("cannot parse %s: %w", hstore, err)
	}

	if c.Nullable {
		if c.TargetDWH == protos.DBType_SNOWFLAKE && len(jsonString) > 15*1024*1024 {
			c.logger.Warn("Clearing HStore equivalent JSON value > 15MB for Snowflake!")
			c.logger.Warn("Check this issue for details: https://github.com/PeerDB-io/peerdb/issues/309")
			return nil, nil
		}
		return &jsonString, nil
	}

	if c.TargetDWH == protos.DBType_SNOWFLAKE && len(jsonString) > 15*1024*1024 {
		c.logger.Warn("Clearing HStore equivalent JSON value > 15MB for Snowflake!")
		c.logger.Warn("Check this issue for details: https://github.com/PeerDB-io/peerdb/issues/309")
		return "", nil
	}
	return jsonString, nil
}

func (c *QValueAvroConverter) processUUID(byteData uuid.UUID, so sizeOpt) (any, int64) {
	uuidString := byteData.String()
	size := stringSize(uuidString, so)
	if c.Nullable {
		return &uuidString, size
	}
	return uuidString, size
}

func (c *QValueAvroConverter) processArrayUUID(arrayData []uuid.UUID, so sizeOpt) (any, int64) {
	if c.Nullable && arrayData == nil {
		return nil, so.nullableSize()
	}

	UUIDData := make([]string, 0, len(arrayData))
	for _, uuid := range arrayData {
		UUIDData = append(UUIDData, uuid.String())
	}
	return UUIDData, stringArraySize(UUIDData, so)
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

// TruncateNumeric truncates a decimal to fit within the target precision and scale.
// Returns the truncated decimal, the number of integer digits, and whether truncation succeeded.
// If ok is false, the value was too large and should be treated as zero.
func TruncateNumeric(
	num decimal.Decimal, targetPrecision, targetScale int16, targetDWH protos.DBType, stat *NumericStat,
) (decimal.Decimal, int, bool) {
	switch targetDWH {
	case protos.DBType_CLICKHOUSE, protos.DBType_SNOWFLAKE, protos.DBType_BIGQUERY:
		bi := num.BigInt()
		bidigi := datatypes.CountDigits(bi)
		if bi.Sign() == 0 {
			bidigi = 0
		}
		if bidigi+int(targetScale) > int(targetPrecision) {
			if stat != nil {
				stat.LongIntegersClearedCount++
				stat.MaxIntegerDigits = max(int32(bidigi), stat.MaxIntegerDigits)
			}
			return decimal.Zero, 0, false
		} else if num.Exponent() < -int32(targetScale) {
			if stat != nil {
				stat.TruncatedCount++
				stat.MaxExponent = max(-num.Exponent(), stat.MaxExponent)
			}
			return num.Truncate(int32(targetScale)), bidigi, true
		}
		return num, bidigi, true
	}
	return num, 0, true
}

//nolint:govet // logically grouped, fieldalignment confuses things
type NumericStat struct {
	DestinationTable         string
	DestinationColumn        string
	TruncatedCount           uint64
	MaxExponent              int32
	LongIntegersClearedCount uint64
	MaxIntegerDigits         int32
	BigInt256ClearedCount    uint64
}

func NewNumericStat(destinationTable, destinationColumn string) NumericStat {
	return NumericStat{
		DestinationTable:  destinationTable,
		DestinationColumn: destinationColumn,
	}
}

func (ns *NumericStat) CollectWarnings(warnings *shared.QRepWarnings) {
	if ns.LongIntegersClearedCount > 0 {
		plural := ""
		if ns.LongIntegersClearedCount > 1 {
			plural = "s"
		}
		err := fmt.Errorf(
			"column %s.%s: cleared %d NUMERIC value%s too big to fit into the destination column (got %d integer digits)",
			ns.DestinationTable, ns.DestinationColumn, ns.LongIntegersClearedCount, plural, ns.MaxIntegerDigits)
		warning := exceptions.NewNumericOutOfRangeError(err, ns.DestinationTable, ns.DestinationColumn)
		*warnings = append(*warnings, warning)
	}
	if ns.TruncatedCount > 0 {
		plural := ""
		if ns.TruncatedCount > 1 {
			plural = "s"
		}
		err := fmt.Errorf(
			"column %s.%s: truncated %d NUMERIC value%s too precise to fit into the destination column (got %d digits of exponent)",
			ns.DestinationTable, ns.DestinationColumn, ns.TruncatedCount, plural, ns.MaxExponent)
		warning := exceptions.NewNumericTruncatedError(err, ns.DestinationTable, ns.DestinationColumn)
		*warnings = append(*warnings, warning)
	}
	if ns.BigInt256ClearedCount > 0 {
		plural := ""
		if ns.BigInt256ClearedCount > 1 {
			plural = "s"
		}
		err := fmt.Errorf(
			"column %s.%s: cleared %d NUMERIC value%s that do not fit into (U)Int256 type in the destination column",
			ns.DestinationTable, ns.DestinationColumn, ns.BigInt256ClearedCount, plural)
		warning := exceptions.NewNumericOutOfRangeError(err, ns.DestinationTable, ns.DestinationColumn)
		*warnings = append(*warnings, warning)
	}
}

func constSize(n int64, so sizeOpt) int64 {
	if so == sizeSkip {
		return 0
	}
	return so.nullableSize() + n
}

func stringSize(s string, so sizeOpt) int64 {
	if so == sizeSkip {
		return 0
	}
	return so.nullableSize() + varIntSize(int64(len(s)), sizePlain) + int64(len(s))
}

func varIntSize(n int64, so sizeOpt) int64 {
	if so == sizeSkip {
		return 0
	}
	if n == 0 {
		return so.nullableSize() + 1
	}
	// Avro uses Variable-Length Zig-Zag encoding
	encoded := uint64((n << 1) ^ (n >> 63))
	bitLen := bits.Len64(encoded)
	return so.nullableSize() + int64((bitLen+6)/7)
}

func fixedArraySize(count int, elemSize int64, so sizeOpt) int64 {
	return arraySize(count, int64(count)*elemSize, so)
}

func varIntArraySize[T ~int16 | ~int32 | ~int64](vals []T, so sizeOpt) int64 {
	if so == sizeSkip {
		return 0
	}
	totalElemSize := int64(0)
	for _, elem := range vals {
		totalElemSize += varIntSize(int64(elem), sizePlain)
	}
	return arraySize(len(vals), totalElemSize, so)
}

func stringArraySize(vals []string, so sizeOpt) int64 {
	if so == sizeSkip {
		return 0
	}
	totalElemSize := int64(0)
	for _, elem := range vals {
		totalElemSize += stringSize(elem, sizePlain)
	}
	return arraySize(len(vals), totalElemSize, so)
}

func arraySize(count int, totalElemSize int64, so sizeOpt) int64 {
	if so == sizeSkip {
		return 0
	}
	// Avro array encoding: block header (negative count + byte size) + elements + termination (varint 0)
	// Block header: varint(-count) + varint(totalElemSize)
	size := so.nullableSize()
	if count > 0 {
		size += varIntSize(int64(-count), sizePlain) + varIntSize(totalElemSize, sizePlain) + totalElemSize
	}
	size += 1 // array termination byte (varint 0)
	return size
}
