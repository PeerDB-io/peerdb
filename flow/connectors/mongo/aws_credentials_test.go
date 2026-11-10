package connmongo

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type countingProvider struct {
	err   error
	creds aws.Credentials
	calls int
}

func (p *countingProvider) Retrieve(context.Context) (aws.Credentials, error) {
	p.calls++
	return p.creds, p.err
}

func TestAWSCredentialsProvider(t *testing.T) {
	t.Run("copies credentials and expiry for the driver", func(t *testing.T) {
		expires := time.Now().Add(time.Hour)
		provider := awsCredentialsProvider{provider: &countingProvider{creds: aws.Credentials{
			AccessKeyID:     "key",
			SecretAccessKey: "secret",
			SessionToken:    "token",
			Source:          "test",
			CanExpire:       true,
			Expires:         expires,
			AccountID:       "123456789012",
		}}}

		creds, err := provider.Retrieve(t.Context())
		require.NoError(t, err)
		assert.Equal(t, "key", creds.AccessKeyID)
		assert.Equal(t, "secret", creds.SecretAccessKey)
		assert.Equal(t, "token", creds.SessionToken)
		assert.Equal(t, "test", creds.Source)
		assert.True(t, creds.CanExpire)
		assert.Equal(t, expires, creds.Expires)
		assert.Equal(t, "123456789012", creds.AccountID)
	})

	t.Run("asks the role chain on every call, leaving caching to it and the driver", func(t *testing.T) {
		inner := &countingProvider{creds: aws.Credentials{AccessKeyID: "key", SecretAccessKey: "secret"}}
		provider := awsCredentialsProvider{provider: inner}

		for range 2 {
			_, err := provider.Retrieve(t.Context())
			require.NoError(t, err)
		}
		assert.Equal(t, 2, inner.calls)
	})

	t.Run("returns the provider's error", func(t *testing.T) {
		assumeRoleErr := errors.New("assume role failed")
		provider := awsCredentialsProvider{provider: &countingProvider{err: assumeRoleErr}}

		_, err := provider.Retrieve(t.Context())
		require.ErrorIs(t, err, assumeRoleErr)
	})
}
