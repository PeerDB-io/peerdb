package utils

import (
	"os"
	"strconv"
)

// GetEnv returns the value of the environment variable with the given name
// and a boolean indicating whether the environment variable exists.
func GetEnv(name string) (string, bool) {
	val, exists := os.LookupEnv(name)
	return val, exists
}

// GetEnvBool returns the value of the environment variable with the given name
// or defaultValue if the environment variable is not set or is not a valid
// boolean value.
func GetEnvBool(name string, defaultValue bool) bool {
	val, ok := GetEnv(name)
	if !ok {
		return defaultValue
	}

	b, err := strconv.ParseBool(val)
	if err != nil {
		return defaultValue
	}

	return b
}

// GetEnvInt returns the value of the environment variable with the given name
// or defaultValue if the environment variable is not set or is not a valid
// integer value.
func GetEnvInt(name string, defaultValue int) int {
	val, ok := GetEnv(name)
	if !ok {
		return defaultValue
	}

	i, err := strconv.Atoi(val)
	if err != nil {
		return defaultValue
	}

	return i
}
