package connmongo

import (
	"math"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/modern-go/reflect2"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// PeerDBExtension provides custom JSON encoding for bson.D types to handle special
// float values (NaN, +Inf, -Inf). Note: custom decoding is not implemented for this
// extension because it is not used in the MongoDB Connector.
type PeerDBExtension struct {
	jsoniter.Extension
}

func (extension *PeerDBExtension) UpdateStructDescriptor(structDescriptor *jsoniter.StructDescriptor) {
	// not used (bson documents does not contain struct)
}

func (extension *PeerDBExtension) CreateMapKeyEncoder(typ reflect2.Type) jsoniter.ValEncoder {
	// use default
	return nil
}

func (extension *PeerDBExtension) CreateEncoder(typ reflect2.Type) jsoniter.ValEncoder {
	// use default
	return nil
}

func (extension *PeerDBExtension) DecorateEncoder(typ reflect2.Type, encoder jsoniter.ValEncoder) jsoniter.ValEncoder {
	if typ.String() == "bson.D" {
		return &BsonDEncoder{}
	}
	// use default
	return encoder
}

type BsonDEncoder struct{}

func (encoder *BsonDEncoder) Encode(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	d := *(*bson.D)(ptr)

	stream.WriteObjectStart()
	for i, elem := range d {
		if i > 0 {
			stream.WriteMore()
		}

		stream.WriteObjectField(elem.Key)

		switch v := elem.Value.(type) {
		case float32:
			if math.IsNaN(float64(v)) {
				stream.WriteString("NaN")
			} else if math.IsInf(float64(v), 1) {
				stream.WriteString("+Inf")
			} else if math.IsInf(float64(v), -1) {
				stream.WriteString("-Inf")
			} else {
				stream.WriteFloat32(v)
			}
		case float64:
			if math.IsNaN(v) {
				stream.WriteString("NaN")
			} else if math.IsInf(v, 1) {
				stream.WriteString("+Inf")
			} else if math.IsInf(v, -1) {
				stream.WriteString("-Inf")
			} else {
				stream.WriteFloat64(v)
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

func (encoder *BsonDEncoder) IsEmpty(ptr unsafe.Pointer) bool {
	d := *(*bson.D)(ptr)
	return len(d) == 0
}

func CreateExtendedJSONMarshaler() jsoniter.API {
	config := jsoniter.ConfigCompatibleWithStandardLibrary
	config.RegisterExtension(&PeerDBExtension{})
	return config
}
