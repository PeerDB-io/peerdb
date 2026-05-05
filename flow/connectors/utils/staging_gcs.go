package utils

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/PeerDB-io/peerdb/flow/internal"
	peerdb_clickhouse "github.com/PeerDB-io/peerdb/flow/pkg/clickhouse"
)

const gcsSignedURLExpiry = 15 * time.Minute

// GCSStagingStore implements StagingStore for Google Cloud Storage using
// native GCS SDK and signed URLs for ClickHouse reads.
type GCSStagingStore struct {
	client   *storage.Client
	bucket   string
	prefix   string
	fullPath string
}

func NewGCSStagingStore(ctx context.Context, bucketPath string) (*GCSStagingStore, error) {
	// bucketPath may use either "gs://" (standard GCS URI scheme) or "gcs://" prefix.
	path := strings.TrimPrefix(bucketPath, "gcs://")
	path = strings.TrimPrefix(path, "gs://")
	bucket, prefix, _ := strings.Cut(path, "/")
	prefix = strings.Trim(prefix, "/")

	// Uses Application Default Credentials (Workload Identity on GKE)
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	return &GCSStagingStore{
		client:   client,
		bucket:   bucket,
		prefix:   prefix,
		fullPath: bucketPath,
	}, nil
}

func (g *GCSStagingStore) Upload(ctx context.Context, env map[string]string, key string, body io.Reader) error {
	logger := internal.LoggerFromCtx(ctx)

	obj := g.client.Bucket(g.bucket).Object(key)
	w := obj.NewWriter(ctx)
	// GCS supports up to 5 TiB objects; the client handles chunked uploads internally.
	// Default chunk size is 16 MiB which is fine for most staging files.

	if _, err := io.Copy(w, body); err != nil {
		w.Close()
		return fmt.Errorf("failed to write to GCS object %s: %w", key, err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to finalize GCS upload for %s: %w", key, err)
	}

	logger.Info("finished GCS upload", slog.String("key", key))
	return nil
}

func (g *GCSStagingStore) TableFunctionExpr(ctx context.Context, key string, format string) (string, error) {
	// Generate a signed URL that ClickHouse can use to read the staged file.
	// Uses Application Default Credentials for signing.
	signedURL, err := g.client.Bucket(g.bucket).SignedURL(key, &storage.SignedURLOptions{
		Method:  "GET",
		Expires: time.Now().Add(gcsSignedURLExpiry),
	})
	if err != nil {
		return "", fmt.Errorf("failed to generate signed URL for gs://%s/%s: %w", g.bucket, key, err)
	}

	var expr strings.Builder
	expr.WriteString("url(")
	expr.WriteString(peerdb_clickhouse.QuoteLiteral(signedURL))
	expr.WriteString(",")
	expr.WriteString(peerdb_clickhouse.QuoteLiteral(format))
	expr.WriteByte(')')
	return expr.String(), nil
}

func (g *GCSStagingStore) DeletePrefix(ctx context.Context, prefix string) error {
	logger := internal.LoggerFromCtx(ctx)
	logger.Info("Deleting objects from GCS",
		slog.String("bucket", g.bucket), slog.String("prefix", prefix))

	it := g.client.Bucket(g.bucket).Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to list GCS objects: %w", err)
		}
		if err := g.client.Bucket(g.bucket).Object(attrs.Name).Delete(ctx); err != nil {
			return fmt.Errorf("failed to delete GCS object %s: %w", attrs.Name, err)
		}
	}

	logger.Info("Deleted objects from GCS",
		slog.String("bucket", g.bucket), slog.String("prefix", prefix))
	return nil
}

func (g *GCSStagingStore) Validate(ctx context.Context) error {
	testKey := g.prefix + "/_peerdb_check_" + time.Now().Format("20060102150405")
	obj := g.client.Bucket(g.bucket).Object(testKey)

	w := obj.NewWriter(ctx)
	if _, err := w.Write([]byte(time.Now().Format(time.RFC3339))); err != nil {
		w.Close()
		return fmt.Errorf("failed to write test object to GCS: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to finalize test object in GCS: %w", err)
	}

	if err := obj.Delete(ctx); err != nil {
		return fmt.Errorf("failed to delete test object from GCS: %w", err)
	}

	return nil
}

func (g *GCSStagingStore) BucketPath() string {
	return g.fullPath
}

func (g *GCSStagingStore) KeyPrefix() string {
	return g.prefix
}

// Bucket returns the bucket name.
func (g *GCSStagingStore) Bucket() string {
	return g.bucket
}

// Prefix returns the key prefix.
func (g *GCSStagingStore) Prefix() string {
	return g.prefix
}
