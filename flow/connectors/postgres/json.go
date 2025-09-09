package connpostgres

import (
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/modern-go/reflect2"
)

type relaxedNumberDecoder struct{}

func (d *relaxedNumberDecoder) Decode(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	anyPtr := (*any)(ptr)
	switch iter.WhatIsNext() {
	case jsoniter.NumberValue:
		numberToken := iter.ReadNumber()
		if val, err := numberToken.Float64(); err == nil {
			*anyPtr = val
		} else {
			*anyPtr = numberToken.String()
		}
	default:
		*anyPtr = iter.Read()
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
