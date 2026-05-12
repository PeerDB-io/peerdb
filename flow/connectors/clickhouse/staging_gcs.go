package connclickhouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/PeerDB-io/peerdb/flow/internal"
	peerdb_clickhouse "github.com/PeerDB-io/peerdb/flow/pkg/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/pkg/objectstore"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

const gcsSignedURLExpiry = 1 * time.Hour

// gcsStagingStore implements StagingStore for Google Cloud Storage using
// native GCS SDK and signed URLs for ClickHouse reads.
type gcsStagingStore struct {
	client   *storage.Client
	bucket   string
	prefix   string
	fullPath string
}

//nolint:iface // factory function intentionally returns interface
func newGCSStagingStore(ctx context.Context, bucketName string) (StagingStore, error) {
	if bucketName == "" {
		return nil, errors.New("PEERDB_CLICKHOUSE_STAGING_BUCKET_NAME must be set when staging provider is gcs")
	}

	deploymentUID := internal.PeerDBDeploymentUID()
	flowName, _ := ctx.Value(shared.FlowNameKey).(string)
	bucketPath := fmt.Sprintf("gs://%s/%s/%s",
		bucketName, url.PathEscape(deploymentUID), url.PathEscape(flowName))

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

	return &gcsStagingStore{
		client:   client,
		bucket:   bucket,
		prefix:   prefix,
		fullPath: bucketPath,
	}, nil
}

func (g *gcsStagingStore) Upload(ctx context.Context, env map[string]string, key string, body io.Reader) error {
	logger := internal.LoggerFromCtx(ctx)

	obj := g.client.Bucket(g.bucket).Object(key)
	w := obj.NewWriter(ctx)

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

func (g *gcsStagingStore) TableFunctionExpr(ctx context.Context, key string, format string) (string, error) {
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

func (g *gcsStagingStore) DeletePrefix(ctx context.Context, prefix string) error {
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

func (g *gcsStagingStore) Validate(ctx context.Context) error {
	return objectstore.NewGCSStagingValidator(g.client, g.bucket, g.prefix)(ctx)
}

func (g *gcsStagingStore) BucketPath() string {
	return g.fullPath
}

func (g *gcsStagingStore) KeyPrefix() string {
	return g.prefix
}
