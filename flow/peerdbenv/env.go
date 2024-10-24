package peerdbenv

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"golang.org/x/exp/constraints"
)

const (
	KMSKeyIDEnvVar = "PEERDB_KMS_KEY_ID"
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

func decryptWithKMS(ctx context.Context, data []byte) ([]byte, error) {
	keyID, exists := os.LookupEnv(KMSKeyIDEnvVar)
	if !exists {
		return data, nil
	}

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	kmsClient := kms.NewFromConfig(cfg)
	decrypted, err := kmsClient.Decrypt(ctx, &kms.DecryptInput{
		CiphertextBlob: data,
		KeyId:          aws.String(keyID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt value: %w", err)
	}

	return decrypted.Plaintext, nil
}

var KMSCache sync.Map

func GetEnvBase64EncodedBytes(ctx context.Context, name string, defaultValue []byte) ([]byte, error) {
	if cacheVal, ok := KMSCache.Load(name); ok {
		if finalVal, ok := cacheVal.([]byte); ok {
			return finalVal, nil
		}
	}

	val, ok := os.LookupEnv(name)
	if !ok {
		return defaultValue, nil
	}

	trimmed := strings.TrimSpace(val)
	decoded, err := base64.StdEncoding.DecodeString(trimmed)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 value for %s: %w", name, err)
	}

	finalVal, err := decryptWithKMS(ctx, decoded)
	if err != nil {
		return finalVal, err
	}
	KMSCache.Store(name, finalVal)
	return finalVal, nil
}

func GetKMSDecryptedEnvString(ctx context.Context, name string, defaultValue string) (string, error) {
	val, ok := os.LookupEnv(name)
	if !ok {
		return defaultValue, nil
	}

	_, exists := os.LookupEnv(KMSKeyIDEnvVar)
	if !exists {
		return val, nil
	}

	ret, err := GetEnvBase64EncodedBytes(ctx, name, []byte(defaultValue))
	if err != nil {
		return defaultValue, fmt.Errorf("failed to get base64 encoded bytes for %s: %w", name, err)
	}

	return string(ret), nil
}
