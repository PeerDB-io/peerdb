package connsnowflake

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/snowflakedb/gosnowflake"
)

// PrepareJWTToken creates a JWT signed with the RSA private key from the Snowflake config.
// The issuer claim format is ACCOUNT.USER.SHA256:base64(sha256(der_public_key)).
func PrepareJWTToken(config *gosnowflake.Config) (string, error) {
	if config.PrivateKey == nil {
		return "", fmt.Errorf("PrivateKey is required for Snowpipe Streaming JWT authentication")
	}

	rsaKey := config.PrivateKey

	pubKeyDER, err := x509.MarshalPKIXPublicKey(&rsaKey.PublicKey)
	if err != nil {
		return "", fmt.Errorf("failed to marshal public key: %w", err)
	}

	pubKeyHash := sha256.Sum256(pubKeyDER)
	pubKeyFP := base64.StdEncoding.EncodeToString(pubKeyHash[:])

	account := extractAccountName(config.Account)
	user := strings.ToUpper(config.User)

	now := time.Now().UTC()
	claims := jwt.MapClaims{
		"iss": fmt.Sprintf("%s.%s.SHA256:%s", account, user, pubKeyFP),
		"sub": fmt.Sprintf("%s.%s", account, user),
		"iat": now.Unix(),
		"exp": now.Add(60 * time.Second).Unix(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	signedToken, err := token.SignedString(rsaKey)
	if err != nil {
		return "", fmt.Errorf("failed to sign JWT token: %w", err)
	}

	return signedToken, nil
}

// GetIngestHost retrieves the Snowpipe Streaming ingest hostname.
// GET https://{account}.snowflakecomputing.com/v2/streaming/hostname
func GetIngestHost(ctx context.Context, jwtToken string, account string, httpClient *http.Client) (string, error) {
	reqURL := fmt.Sprintf("https://%s.snowflakecomputing.com/v2/streaming/hostname", account)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create ingest host request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+jwtToken)
	req.Header.Set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
	req.Header.Set("Accept", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to get ingest host: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read ingest host response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("ingest host request failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Response is a JSON string; replace underscores with hyphens
	host := strings.TrimSpace(string(body))
	host = strings.Trim(host, "\"")
	host = strings.ReplaceAll(host, "_", "-")

	return host, nil
}

// GetScopedToken exchanges a JWT for a scoped OAuth token for the ingest host.
// POST https://{account}.snowflakecomputing.com/oauth/token
func GetScopedToken(ctx context.Context, jwtToken string, account string, ingestHost string, httpClient *http.Client) (string, time.Time, error) {
	reqURL := fmt.Sprintf("https://%s.snowflakecomputing.com/oauth/token", account)

	formData := url.Values{
		"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"},
		"scope":      {ingestHost},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, strings.NewReader(formData.Encode()))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to create scoped token request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "Bearer "+jwtToken)

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to get scoped token: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to read scoped token response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", time.Time{}, fmt.Errorf("scoped token request failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Response is a raw JWT string, not a JSON-wrapped {"access_token": "..."} object.
	tokenStr := strings.TrimSpace(string(body))

	// Parse the JWT to extract expiry
	parser := jwt.NewParser(jwt.WithoutClaimsValidation())
	parsed, _, err := parser.ParseUnverified(tokenStr, jwt.MapClaims{})
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to parse scoped token JWT: %w", err)
	}

	expiry, err := parsed.Claims.GetExpirationTime()
	if err != nil || expiry == nil {
		// Default to 55 minutes from now if we can't parse expiry
		return tokenStr, time.Now().Add(55 * time.Minute), nil
	}

	return tokenStr, expiry.Time, nil
}

// extractAccountName strips region suffixes and uppercases the account name.
// e.g., "myaccount.us-east-1" → "MYACCOUNT"
func extractAccountName(rawAccount string) string {
	parts := strings.SplitN(rawAccount, ".", 2)
	return strings.ToUpper(parts[0])
}
