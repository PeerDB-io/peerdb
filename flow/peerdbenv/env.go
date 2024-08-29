package peerdbenv

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kms"
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

func GetEnvBase64EncodedBytes(name string, defaultValue []byte) ([]byte, error) {
	val, ok := os.LookupEnv(name)
	if !ok {
		return defaultValue, nil
	}

	trimmed := strings.TrimSpace(val)
	return base64.StdEncoding.DecodeString(trimmed)
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

func GetKMSDecryptedEnvString(name string, defaultValue string) (string, error) {
	keyID, exists := os.LookupEnv("PEERDB_KMS_KEY_ID")
	if !exists {
		// assume it's unencrypted
		return GetEnvString(name, defaultValue), nil
	}

	encryptedBase64, exists := os.LookupEnv(name)
	if !exists {
		return defaultValue, nil
	}

	encryptedBytes, err := base64.StdEncoding.DecodeString(encryptedBase64)
	if err != nil {
		return "", fmt.Errorf("failed to decode encrypted value for %s: %w", name, err)
	}

	sess, err := session.NewSession()
	if err != nil {
		return "", fmt.Errorf("failed to create AWS session for decrypting %s: %w", name, err)
	}

	kmsClient := kms.New(sess)
	decrypted, err := kmsClient.Decrypt(&kms.DecryptInput{
		CiphertextBlob: encryptedBytes,
		KeyId:          aws.String(keyID),
	})
	if err != nil {
		return "", fmt.Errorf("failed to decrypt value for %s: %w", name, err)
	}

	return string(decrypted.Plaintext), nil
}
