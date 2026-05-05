package connclickhouse

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// createStagingStore builds a StagingStore and (for S3) the legacy ClickHouseS3Credentials.
// The credsProvider return is non-nil only for S3 and is kept for backward compatibility
// with the session-token version check in NewClickHouseConnector.
func createStagingStore(
	ctx context.Context,
	env map[string]string,
	config *protos.ClickhouseConfig,
) (utils.StagingStore, *utils.ClickHouseS3Credentials, error) {
	// If user provided explicit S3 config, always use S3 staging.
	if config.S3 != nil || config.S3Path != "" || config.AccessKeyId != "" {
		return createS3StagingStore(ctx, env, config, "")
	}

	// Check environment-based staging provider config.
	provider, err := internal.PeerDBClickHouseStagingProvider(ctx, env)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get staging provider: %w", err)
	}

	// Prefer unified PEERDB_CLICKHOUSE_STAGING_BUCKET_NAME, fall back to provider-specific env vars.
	bucketName, err := internal.PeerDBClickHouseStagingBucketName(ctx, env)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get staging bucket name: %w", err)
	}

	switch strings.ToLower(provider) {
	case "gcs":
		store, err := createGCSStagingStore(ctx, bucketName)
		return store, nil, err
	default:
		return createS3StagingStore(ctx, env, config, bucketName)
	}
}

func createS3StagingStore(
	ctx context.Context,
	env map[string]string,
	config *protos.ClickhouseConfig,
	unifiedBucketName string,
) (utils.StagingStore, *utils.ClickHouseS3Credentials, error) {
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
		return nil, nil, err
	}

	if awsBucketPath == "" {
		deploymentUID := internal.PeerDBDeploymentUID()
		flowName, _ := ctx.Value(shared.FlowNameKey).(string)
		bucketPathSuffix := fmt.Sprintf("%s/%s", url.PathEscape(deploymentUID), url.PathEscape(flowName))
		// Prefer unified bucket name, fall back to legacy S3-specific env var.
		awsBucketName := unifiedBucketName
		if awsBucketName == "" {
			awsBucketName, err = internal.PeerDBClickHouseAWSS3BucketName(ctx, env)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to get PeerDB ClickHouse Bucket Name: %w", err)
			}
		}
		if awsBucketName == "" {
			return nil, nil, errors.New("PeerDB ClickHouse Bucket Name not set")
		}
		awsBucketPath = fmt.Sprintf("s3://%s/%s", awsBucketName, bucketPathSuffix)
	}

	creds := &utils.ClickHouseS3Credentials{
		Provider:   credentialsProvider,
		BucketPath: awsBucketPath,
	}

	store, err := utils.NewS3StagingStore(awsBucketPath, credentialsProvider)
	if err != nil {
		return nil, nil, err
	}

	return store, creds, nil
}

func createGCSStagingStore(
	ctx context.Context,
	bucketName string,
) (utils.StagingStore, error) {
	if bucketName == "" {
		return nil, errors.New("PEERDB_CLICKHOUSE_STAGING_BUCKET_NAME must be set when staging provider is gcs")
	}

	deploymentUID := internal.PeerDBDeploymentUID()
	flowName, _ := ctx.Value(shared.FlowNameKey).(string)
	bucketPath := fmt.Sprintf("gs://%s/%s/%s",
		bucketName, url.PathEscape(deploymentUID), url.PathEscape(flowName))

	return utils.NewGCSStagingStore(ctx, bucketPath)
}
