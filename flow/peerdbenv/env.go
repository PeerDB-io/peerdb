package peerdbenv

import (
	"encoding/json"
	"os"
	"reflect"
	"strconv"

	"golang.org/x/exp/constraints"
)

// GetEnvInt returns the value of the environment variable with the given name
// or defaultValue if the environment variable is not set or is not a valid value.
func getEnvInt(name string, defaultValue int) int {
	val, ok := os.LookupEnv(name)
	if !ok {
		return defaultValue
	}

	i, err := strconv.Atoi(val)
	if err != nil {
		return defaultValue
	}

	return i
}

// getEnvUint returns the value of the environment variable with the given name
// or defaultValue if the environment variable is not set or is not a valid value.
func getEnvUint[T constraints.Unsigned](name string, defaultValue T) T {
	val, ok := os.LookupEnv(name)
	if !ok {
		return defaultValue
	}

	// widest bit size, truncate later
	i, err := strconv.ParseUint(val, 10, int(reflect.TypeFor[T]().Size()*8))
	if err != nil {
		return defaultValue
	}

	return T(i)
}

// GetEnvString returns the value of the environment variable with the given name
// or defaultValue if the environment variable is not set.
func GetEnvString(name string, defaultValue string) string {
	val, ok := os.LookupEnv(name)
	if !ok {
		return defaultValue
	}

	return val
}

func GetEnvJSON[T any](name string, defaultValue T) T {
	val, ok := os.LookupEnv(name)
	if !ok {
		return defaultValue
	}

	var result T
	if err := json.Unmarshal([]byte(val), &result); err != nil {
		return defaultValue
	}

	return result
}
