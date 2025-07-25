package internal

import (
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
	"google.golang.org/protobuf/proto"
)

func ProtoUnmarshal(b []byte, m proto.Message) error {
	if err := proto.Unmarshal(b, m); err != nil {
		return exceptions.NewProtoUnmarshalError(err)
	}
	return nil
}

func ProtoMarshal(m proto.Message) ([]byte, error) {
	if bytes, err := proto.Marshal(m); err != nil {
		return nil, exceptions.NewProtoMarshalError(err)
	} else {
		return bytes, nil
	}
}
