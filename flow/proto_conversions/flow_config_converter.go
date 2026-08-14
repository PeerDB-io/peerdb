// Package proto_conversions converts between the API-facing FlowConnectionConfigs
// and the internal FlowConnectionConfigsCore.
//
// The two messages are wire-compatible by construction - every field they share
// has the same number and type - so conversion is a copy by field number rather
// than a hand-maintained (or generated) field list. TestFlowConnectionConfigsEquivalence
// enforces that invariant.
package proto_conversions

import (
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// FlowConnectionConfigsToCore converts API FlowConnectionConfigs to internal FlowConnectionConfigsCore
func FlowConnectionConfigsToCore(api *protos.FlowConnectionConfigs) *protos.FlowConnectionConfigsCore {
	if api == nil {
		return nil
	}

	core := &protos.FlowConnectionConfigsCore{}
	copyFieldsByNumber(api.ProtoReflect(), core.ProtoReflect())
	return core
}

// copyFieldsByNumber copies every set field of src onto the field with the same
// number in dst, skipping numbers dst doesn't declare (fields dropped from
// FlowConnectionConfigsCore). Composite values are shared, not deep-copied.
//
// Oneofs need no special handling: protoc-gen-go gives each parent message its
// own wrapper type for the same oneof, but Set resolves the wrapper from dst's
// own descriptor.
func copyFieldsByNumber(src, dst protoreflect.Message) {
	srcFields := src.Descriptor().Fields()
	dstFields := dst.Descriptor().Fields()
	for i := range srcFields.Len() {
		fd := srcFields.Get(i)
		if !src.Has(fd) {
			continue
		}
		if dstFd := dstFields.ByNumber(fd.Number()); dstFd != nil {
			dst.Set(dstFd, src.Get(fd))
		}
	}
}
