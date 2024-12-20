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
	KmsKeyIDEnvVar = "PEERDB_KMS_KEY_ID"
)

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

func getEnvConvert[T any](name string, defaultValue T, convert func(string) (T, error)) T {
	val, ok := os.LookupEnv(name)
	if !ok {
		return defaultValue
	}

	ret, err := convert(val)
	if err != nil {
		return defaultValue
	}
	return ret
}

func decryptWithKms(ctx context.Context, data []byte) ([]byte, error) {
	keyID, exists := os.LookupEnv(KmsKeyIDEnvVar)
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

var kmsCache sync.Map

func GetKmsDecryptedEnvBase64EncodedBytes(ctx context.Context, name string, defaultValue []byte) ([]byte, error) {
	if cacheVal, ok := kmsCache.Load(name); ok {
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

	finalVal, err := decryptWithKms(ctx, decoded)
	if err != nil {
		return finalVal, err
	}
	kmsCache.Store(name, finalVal)
	return finalVal, nil
}

func GetKmsDecryptedEnvString(ctx context.Context, name string, defaultValue string) (string, error) {
	val, ok := os.LookupEnv(name)
	if !ok {
		return defaultValue, nil
	}

	_, exists := os.LookupEnv(KmsKeyIDEnvVar)
	if !exists {
		return val, nil
	}

	ret, err := GetKmsDecryptedEnvBase64EncodedBytes(ctx, name, []byte(defaultValue))
	if err != nil {
		return defaultValue, fmt.Errorf("failed to get base64 encoded bytes for %s: %w", name, err)
	}

	return string(ret), nil
}
