package util

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	mathRand "math/rand"
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

// RandomString generates a random alphanumeric string of given length.
// NOTE: This function uses math/rand which is not cryptographically random;
// this function is for non-security purposes.
// crypto/rand's rand.Read could potentially error, which is not
// desirable for this function's use case.
func RandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		result[i] = charset[mathRand.Intn(len(charset))]
	}
	return string(result)
}
