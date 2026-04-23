package internal

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
	KmsKeyIDEnvVar    = "PEERDB_KMS_KEY_ID"
	KmsProviderEnvVar = "PEERDB_KMS_PROVIDER"
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

func GetEnvBool(name string, defaultValue bool) bool {
	return getEnvConvert(name, defaultValue, strconv.ParseBool)
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

	provider := GetEnvString(KmsProviderEnvVar, "aws")
	switch provider {
	case "gcp":
		return decryptWithGcpKms(ctx, data, keyID)
	default:
		return decryptWithAwsKms(ctx, data, keyID)
	}
}

func decryptWithAwsKms(ctx context.Context, data []byte, keyID string) ([]byte, error) {
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

func decryptWithGcpKms(ctx context.Context, data []byte, keyID string) ([]byte, error) {
	// Get access token from GKE Workload Identity metadata server
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata request: %w", err)
	}
	req.Header.Set("Metadata-Flavor", "Google")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get GCP access token: %w", err)
	}
	defer resp.Body.Close()

	var tokenResp struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return nil, fmt.Errorf("failed to parse GCP token response: %w", err)
	}

	// Call Cloud KMS decrypt API
	ciphertext := base64.StdEncoding.EncodeToString(data)
	body := fmt.Sprintf(`{"ciphertext":"%s"}`, ciphertext)

	kmsURL := fmt.Sprintf("https://cloudkms.googleapis.com/v1/%s:decrypt", keyID)
	kmsReq, err := http.NewRequestWithContext(ctx, http.MethodPost, kmsURL, strings.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create KMS request: %w", err)
	}
	kmsReq.Header.Set("Authorization", "Bearer "+tokenResp.AccessToken)
	kmsReq.Header.Set("Content-Type", "application/json")

	kmsResp, err := http.DefaultClient.Do(kmsReq)
	if err != nil {
		return nil, fmt.Errorf("failed to call GCP KMS: %w", err)
	}
	defer kmsResp.Body.Close()

	if kmsResp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(kmsResp.Body)
		return nil, fmt.Errorf("GCP KMS decrypt failed (%d): %s", kmsResp.StatusCode, string(respBody))
	}

	var kmsResult struct {
		Plaintext string `json:"plaintext"`
	}
	if err := json.NewDecoder(kmsResp.Body).Decode(&kmsResult); err != nil {
		return nil, fmt.Errorf("failed to parse GCP KMS response: %w", err)
	}

	return base64.StdEncoding.DecodeString(kmsResult.Plaintext)
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
