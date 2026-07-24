package connclickhouse

import (
	"context"
	"io"
)

const stagingFormat = "Avro"

// StagingStore abstracts cloud storage used for staging Avro files.
// Files are written by PeerDB and read by ClickHouse via table functions
// (s3(), url(), etc.).
type StagingStore interface {
	// Upload streams data from body to the given key in the staging bucket.
	Upload(ctx context.Context, env map[string]string, key string, body io.Reader) error

	// TableFunctionExpr returns a ClickHouse SQL expression that reads the staged file.
	// For S3:  s3('url', 'access_key', 'secret_key', 'Avro')
	// For GCS: url('signed_url', 'Avro')
	TableFunctionExpr(ctx context.Context, key string, format string) (string, error)

	// DeletePrefix removes all objects whose key starts with prefix.
	DeletePrefix(ctx context.Context, prefix string) error

	// Validate checks that the store is writable by putting and removing a test object.
	Validate(ctx context.Context) error

	// BucketPath returns the full staging path (e.g. "s3://bucket/prefix") for logging.
	BucketPath() string

	// KeyPrefix returns the object key prefix within the bucket.
	KeyPrefix() string
}
