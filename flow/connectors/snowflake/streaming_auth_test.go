package connsnowflake

import (
	"crypto/rand"
	"crypto/rsa"
	"testing"

	"github.com/snowflakedb/gosnowflake"
)

func TestPrepareJWTToken_Success(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate RSA key: %v", err)
	}

	config := &gosnowflake.Config{
		Account:    "testaccount.us-east-1",
		User:       "testuser",
		PrivateKey: privateKey,
	}

	token, err := PrepareJWTToken(config)
	if err != nil {
		t.Fatalf("PrepareJWTToken failed: %v", err)
	}

	if len(token) < 100 {
		t.Errorf("expected token length > 100, got %d", len(token))
	}
}

func TestPrepareJWTToken_NoPrivateKey(t *testing.T) {
	config := &gosnowflake.Config{
		Account: "testaccount",
		User:    "testuser",
	}

	_, err := PrepareJWTToken(config)
	if err == nil {
		t.Fatal("expected error when PrivateKey is nil")
	}

	if got := err.Error(); got != "PrivateKey is required for Snowpipe Streaming JWT authentication" {
		t.Errorf("unexpected error message: %s", got)
	}
}

func TestExtractAccountName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"myaccount.us-east-1", "MYACCOUNT"},
		{"myaccount", "MYACCOUNT"},
		{"MyAccount.us-west-2.aws", "MYACCOUNT"},
		{"ALREADY_UPPER", "ALREADY_UPPER"},
	}

	for _, tt := range tests {
		got := extractAccountName(tt.input)
		if got != tt.expected {
			t.Errorf("extractAccountName(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}
