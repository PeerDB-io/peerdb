package connmongo

import (
	"math"
	"reflect"
	"strconv"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/modern-go/reflect2"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// BsonExtension provides custom JSON encoding/decoding for MongoDB BSON types.
// The main purpose is to handle special float values (NaN, +Inf, -Inf) in bson.D and bson.A
// by converting them to JSON strings. Decoders are implemented to satisfy json-iterator's
// extension interface requirements, even though decoding is not used by MongoDB Connector.
type BsonExtension struct {
	jsoniter.DummyExtension
}

func (extension *BsonExtension) DecorateEncoder(typ reflect2.Type, encoder jsoniter.ValEncoder) jsoniter.ValEncoder {
	if typ.Type1() == reflect.TypeFor[bson.D]() {
		return &BsonDCodec{}
	}
	if typ.Type1() == reflect.TypeFor[bson.A]() {
		return &BsonACodec{}
	}
	// use default
	return encoder
}

type BsonDCodec struct{}

func (codec *BsonDCodec) Encode(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	d := *(*bson.D)(ptr)

	stream.WriteObjectStart()
	for i, elem := range d {
		if i > 0 {
			stream.WriteMore()
		}

		stream.WriteObjectField(elem.Key)

		switch v := elem.Value.(type) {
		case float64:
			if math.IsNaN(v) {
				stream.WriteString("NaN")
			} else if math.IsInf(v, 1) {
				stream.WriteString("+Inf")
			} else if math.IsInf(v, -1) {
				stream.WriteString("-Inf")
			} else {
				writeFloat64WithExplicitDecimal(v, stream)
			}
		default:
			// Delegate to json-iterator's encoding system for all other types.
			// This maintains extension behavior: if elem.Value is another bson.D,
			// it will recursively use this custom encoder for proper NaN/Inf handling.
			stream.WriteVal(elem.Value)
		}
	}
	stream.WriteObjectEnd()
}

func (codec *BsonDCodec) IsEmpty(ptr unsafe.Pointer) bool {
	d := *(*bson.D)(ptr)
	return len(d) == 0
}

func (codec *BsonDCodec) Decode(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	// Not used
}

type BsonACodec struct{}

func (codec *BsonACodec) Encode(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	a := *(*bson.A)(ptr)

	stream.WriteArrayStart()
	for i, elem := range a {
		if i > 0 {
			stream.WriteMore()
		}
		switch v := elem.(type) {
		case float64:
			if math.IsNaN(v) {
				stream.WriteString("NaN")
			} else if math.IsInf(v, 1) {
				stream.WriteString("+Inf")
			} else if math.IsInf(v, -1) {
				stream.WriteString("-Inf")
			} else {
				writeFloat64WithExplicitDecimal(v, stream)
			}
		default:
			// Delegate to json-iterator's encoding system for all other types.
			// This maintains extension behavior: if elem.Value is another bson.A,
			// it will recursively use this custom encoder for proper NaN/Inf handling.
			stream.WriteVal(elem)
		}
	}
	stream.WriteArrayEnd()
}

func (codec *BsonACodec) IsEmpty(ptr unsafe.Pointer) bool {
	a := *(*bson.A)(ptr)
	return len(a) == 0
}

func (codec *BsonACodec) Decode(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	// Not used
}

func CreateExtendedJSONMarshaler() jsoniter.API {
	config := jsoniter.ConfigCompatibleWithStandardLibrary
	config.RegisterExtension(&BsonExtension{})
	return config
}

// Assume (and test) that values outside of these limits will come out in scientific notation
// and will be parsed as floats either way
var (
	floatLimit    = math.Pow10(21)
	floatNegLimit = -floatLimit
)

func writeFloat64WithExplicitDecimal(v float64, stream *jsoniter.Stream) {
	if v < floatLimit && v > floatNegLimit && v == math.Trunc(v) {
		// use explicit decimal to hint ClickHouse to parse as float
		stream.WriteRaw(strconv.FormatFloat(v, 'f', 1, 64))
	} else {
		stream.WriteFloat64(v)
	}
}
