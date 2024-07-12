package shared

import "google.golang.org/protobuf/proto"

func CloneProto[T proto.Message](msg T) T {
	return proto.Clone(msg).(T)
}
