package connclickhouse

import (
	"context"
	"io"
)

const stagingFormat = "Avro"

// stagingCheckObjectPrefix is the basename used for temporary objects written
// during a staging-bucket smoke test. Kept short and unambiguous so it's easy
// to spot in bucket listings if a test object is ever leaked.
const stagingCheckObjectPrefix = "_peerdb_check_"

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

	// MultiKeyTableFunctionExpr returns a ClickHouse SQL expression that reads
	// all given staged files as one relation, e.g. to insert several batches
	// with a single INSERT SELECT instead of one per batch.
	// For S3:  s3(['url1', 'url2'], 'access_key', 'secret_key', 'Avro')
	// For GCS: url(['signed_url1', 'signed_url2'], 'Avro')
	MultiKeyTableFunctionExpr(ctx context.Context, keys []string, format string) (string, error)

	// DeletePrefix removes all objects whose key starts with prefix.
	DeletePrefix(ctx context.Context, prefix string) error

	// Validate checks that the store is writable by putting and removing a test object.
	Validate(ctx context.Context) error

	// ClickHouseAccessMethod returns the access type ClickHouse uses to read staged files.
	// This is S3 for direct S3 reads and URL for signed-URL reads.
	ClickHouseAccessMethod() string

	// BucketPath returns the full staging path (e.g. "s3://bucket/prefix") for logging.
	BucketPath() string

	// KeyPrefix returns the object key prefix within the bucket.
	KeyPrefix() string
}
