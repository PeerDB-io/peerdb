package utils

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"
)

func TestNewS3StagingStore(t *testing.T) {
	creds := NewStaticAWSCredentialsProvider(AWSCredentials{
		AWS: aws.Credentials{
			AccessKeyID:     "test-key",
			SecretAccessKey: "test-secret",
		},
	}, "us-east-1", nil, "")

	store, err := NewS3StagingStore("s3://my-bucket/some/prefix", creds)
	require.NoError(t, err)
	require.Equal(t, "some/prefix", store.KeyPrefix())
	require.Equal(t, "s3://my-bucket/some/prefix", store.BucketPath())
}

func TestS3StagingStoreTableFunctionExpr(t *testing.T) {
	creds := NewStaticAWSCredentialsProvider(AWSCredentials{
		AWS: aws.Credentials{
			AccessKeyID:     "AKID",
			SecretAccessKey: "SECRET",
		},
	}, "us-east-1", nil, "")

	store, err := NewS3StagingStore("s3://my-bucket/prefix", creds)
	require.NoError(t, err)

	expr, err := store.TableFunctionExpr(context.Background(), "prefix/flow/file.avro", "Avro")
	require.NoError(t, err)

	// Should produce an s3() table function with proper URL and credentials
	require.True(t, strings.HasPrefix(expr, "s3("))
	require.Contains(t, expr, "my-bucket")
	require.Contains(t, expr, "AKID")
	require.Contains(t, expr, "SECRET")
	require.Contains(t, expr, "'Avro'")
}

func TestS3StagingStoreTableFunctionExprWithEndpoint(t *testing.T) {
	endpoint := "https://storage.googleapis.com"
	creds := NewStaticAWSCredentialsProvider(AWSCredentials{
		AWS: aws.Credentials{
			AccessKeyID:     "HMAC_KEY",
			SecretAccessKey: "HMAC_SECRET",
		},
		EndpointUrl: &endpoint,
	}, "auto", nil, "")

	store, err := NewS3StagingStore("s3://gcs-bucket/prefix", creds)
	require.NoError(t, err)

	expr, err := store.TableFunctionExpr(context.Background(), "prefix/flow/file.avro", "Avro")
	require.NoError(t, err)

	// With a custom endpoint, URL should use endpoint format, not AWS format
	require.True(t, strings.HasPrefix(expr, "s3("))
	require.Contains(t, expr, "storage.googleapis.com")
	require.Contains(t, expr, "HMAC_KEY")
	require.Contains(t, expr, "'Avro'")
}

func TestS3StagingStoreTableFunctionExprWithSessionToken(t *testing.T) {
	creds := NewStaticAWSCredentialsProvider(AWSCredentials{
		AWS: aws.Credentials{
			AccessKeyID:     "AKID",
			SecretAccessKey: "SECRET",
			SessionToken:    "TOKEN123",
		},
	}, "us-west-2", nil, "")

	store, err := NewS3StagingStore("s3://bucket/prefix", creds)
	require.NoError(t, err)

	expr, err := store.TableFunctionExpr(context.Background(), "prefix/file.avro", "Avro")
	require.NoError(t, err)

	// Session token should be included as 4th argument
	require.Contains(t, expr, "TOKEN123")
}

func TestNewS3StagingStoreNoBucket(t *testing.T) {
	creds := NewStaticAWSCredentialsProvider(AWSCredentials{}, "", nil, "")

	store, err := NewS3StagingStore("s3://bucket-only", creds)
	require.NoError(t, err)
	require.Equal(t, "s3://bucket-only", store.BucketPath())
	require.Empty(t, store.KeyPrefix())
}
