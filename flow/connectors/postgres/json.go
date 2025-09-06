package connpostgres

import (
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/modern-go/reflect2"
)

type relaxedNumberDecoder struct{}

func (d *relaxedNumberDecoder) Decode(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	switch iter.WhatIsNext() {
	case jsoniter.NumberValue:
		numberToken := iter.ReadNumber()
		if val, err := numberToken.Float64(); err == nil {
			*(*any)(ptr) = val
		} else {
			*(*any)(ptr) = numberToken.String()
		}
	default:
		*(*any)(ptr) = iter.Read()
	}
}

type RelaxedNumberExtension struct {
	jsoniter.DummyExtension
}

func (extension *RelaxedNumberExtension) CreateDecoder(typ reflect2.Type) jsoniter.ValDecoder {
	if typ == reflect2.TypeOfPtr((*any)(nil)).Elem() {
		return &relaxedNumberDecoder{}
	}
	return nil
}

func createExtendedJSONUnmarshaler() jsoniter.API {
	config := jsoniter.ConfigCompatibleWithStandardLibrary
	config.RegisterExtension(&RelaxedNumberExtension{})
	return config
}
