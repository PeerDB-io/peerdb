package connclickhouse

import (
	"context"
	"fmt"
	"strings"

	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

func createStagingStore(
	ctx context.Context,
	env map[string]string,
	config *protos.ClickhouseConfig,
	chVersion clickhouseproto.Version,
) (StagingStore, error) {
	provider, err := internal.PeerDBClickHouseStagingProvider(ctx, env)
	if err != nil {
		return nil, fmt.Errorf("failed to get staging provider: %w", err)
	}

	// Prefer unified PEERDB_CLICKHOUSE_STAGING_BUCKET_NAME, fall back to legacy S3-specific env var.
	bucketName, err := internal.PeerDBClickHouseStagingBucketName(ctx, env)
	if err != nil {
		return nil, fmt.Errorf("failed to get staging bucket name: %w", err)
	}
	if bucketName == "" {
		bucketName, err = internal.PeerDBClickHouseAWSS3BucketName(ctx, env)
		if err != nil {
			return nil, fmt.Errorf("failed to get S3 bucket name: %w", err)
		}
	}

	switch strings.ToLower(provider) {
	case "s3", "":
		return newS3StagingStore(ctx, config, bucketName, chVersion)
	case "gcs":
		return newGCSStagingStore(ctx, bucketName)
	default:
		return nil, fmt.Errorf("unsupported staging provider %q (expected s3 or gcs)", provider)
	}
}
