package connclickhouse

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
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
		return newS3StagingStoreFromConfig(ctx, config, bucketName, chVersion)
	case "gcs":
		return newGCSStagingStore(ctx, bucketName)
	default:
		return nil, fmt.Errorf("unsupported staging provider %q (expected s3 or gcs)", provider)
	}
}

func newS3StagingStoreFromConfig(
	ctx context.Context,
	config *protos.ClickhouseConfig,
	unifiedBucketName string,
	chVersion clickhouseproto.Version,
) (StagingStore, error) {
	var awsConfig utils.PeerAWSCredentials
	var awsBucketPath string
	if config.S3 != nil {
		awsConfig = utils.NewPeerAWSCredentials(config.S3)
		awsBucketPath = config.S3.Url
	} else {
		awsConfig = utils.PeerAWSCredentials{
			Credentials: aws.Credentials{
				AccessKeyID:     config.AccessKeyId,
				SecretAccessKey: config.SecretAccessKey,
			},
			EndpointUrl: config.Endpoint,
			Region:      config.Region,
		}
		awsBucketPath = config.S3Path
	}

	credentialsProvider, err := utils.GetAWSCredentialsProvider(ctx, "clickhouse", awsConfig)
	if err != nil {
		return nil, err
	}

	if awsBucketPath == "" {
		if unifiedBucketName == "" {
			return nil, errors.New("PeerDB ClickHouse Bucket Name not set")
		}
		deploymentUID := internal.PeerDBDeploymentUID()
		flowName, _ := ctx.Value(shared.FlowNameKey).(string)
		bucketPathSuffix := fmt.Sprintf("%s/%s", url.PathEscape(deploymentUID), url.PathEscape(flowName))
		awsBucketPath = fmt.Sprintf("s3://%s/%s", unifiedBucketName, bucketPathSuffix)
	}

	// S3 with session tokens requires ClickHouse >= 24.3.1
	// https://github.com/ClickHouse/ClickHouse/issues/61230
	credentials, err := credentialsProvider.Retrieve(ctx)
	if err != nil {
		return nil, err
	}
	if credentials.AWS.SessionToken != "" {
		if !clickhouseproto.CheckMinVersion(
			clickhouseproto.Version{Major: 24, Minor: 3, Patch: 1},
			chVersion,
		) {
			return nil, fmt.Errorf(
				"provide S3 Transient Stage details explicitly or upgrade to ClickHouse version >= 24.3.1, current version is %s. %s",
				chVersion,
				"You can also contact PeerDB support for implicit S3 stage setup for older versions of ClickHouse.")
		}
	}

	return newS3StagingStore(awsBucketPath, credentialsProvider)
}

func newGCSStagingStore(ctx context.Context, bucketName string) (StagingStore, error) {
	if bucketName == "" {
		return nil, errors.New("PEERDB_CLICKHOUSE_STAGING_BUCKET_NAME must be set when staging provider is gcs")
	}

	deploymentUID := internal.PeerDBDeploymentUID()
	flowName, _ := ctx.Value(shared.FlowNameKey).(string)
	bucketPath := fmt.Sprintf("gs://%s/%s/%s",
		bucketName, url.PathEscape(deploymentUID), url.PathEscape(flowName))

	return newGCSStagingStoreFromPath(ctx, bucketPath)
}
