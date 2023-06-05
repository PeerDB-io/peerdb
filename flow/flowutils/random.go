package flowutils

import (
	"encoding/binary"

	"github.com/google/uuid"
)

func GetRandomUInt64() uint64 {
	// Generate a new UUID
	id := uuid.New()

	// Use the first 8 bytes of the UUID
	return binary.BigEndian.Uint64(id[:8])
}

func GetRandomInt64() int64 {
	// Generate a new UUID
	id := uuid.New()

	// Use the first 8 bytes of the UUID
	return int64(binary.BigEndian.Uint64(id[:8]))
}
