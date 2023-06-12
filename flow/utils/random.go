package util

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
)

// RandomInt64 returns a random 64 bit integer.
func RandomInt64() (int64, error) {
	b := make([]byte, 8)
	_, err := rand.Read(b)
	if err != nil {
		return 0, errors.New("could not generate random int64: " + err.Error())
	}
	// Convert bytes to int64
	return int64(binary.LittleEndian.Uint64(b)), nil
}

// RandomUInt64 returns a random 64 bit unsigned integer.
func RandomUInt64() (uint64, error) {
	b := make([]byte, 8)
	_, err := rand.Read(b)
	if err != nil {
		return 0, errors.New("could not generate random uint64: " + err.Error())
	}
	// Convert bytes to uint64
	return binary.LittleEndian.Uint64(b), nil
}
