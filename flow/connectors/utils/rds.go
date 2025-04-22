package utils

import (
	"context"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

// RDSAuthTokenTTL is the cache TTL for RDS auth tokens. RDS Tokens Live for 15 minutes by default
const RDSAuthTokenTTL = 10 * time.Minute

type RDSAuth struct {
	updateTime    time.Time
	AwsAuthConfig *protos.AwsAuthenticationConfig
	token         string
	lock          sync.Mutex
}

type RDSConnectionConfig struct {
	Host string
	User string
	Port uint32
}

func BuildPeerAWSCredentials(awsAuth *protos.AwsAuthenticationConfig) PeerAWSCredentials {
	switch config := awsAuth.AuthConfig.(type) {
	case *protos.AwsAuthenticationConfig_StaticCredentials:
		return PeerAWSCredentials{
			Credentials: aws.Credentials{
				AccessKeyID:     *config.StaticCredentials.AccessKeyId,
				SecretAccessKey: *config.StaticCredentials.SecretAccessKey,
			},
			Region: awsAuth.Region,
		}
	case *protos.AwsAuthenticationConfig_Role:
		return PeerAWSCredentials{
			RoleArn:        &config.Role.RoleArn,
			ChainedRoleArn: config.Role.ChainedRoleArn,
			Region:         awsAuth.Region,
		}
	}
	return PeerAWSCredentials{}
}

var regionRegex = regexp.MustCompile(`^.*?\..*?\.([a-z0-9-]+)\.rds\.amazonaws\.com$`)

func GetRDSToken(ctx context.Context, connConfig RDSConnectionConfig, rdsAuth *RDSAuth, connectorName string) (string, error) {
	logger := internal.LoggerFromCtx(ctx)
	now := time.Now()
	if rdsAuth.updateTime.Add(RDSAuthTokenTTL).After(now) && rdsAuth.token != "" {
		logger.Info("Using cached RDS token for connector: " + connectorName)
		return rdsAuth.token, nil
	}
	return func() (string, error) {
		logger.Info("Generating new RDS token for connector: " + connectorName)
		rdsAuth.lock.Lock()
		defer rdsAuth.lock.Unlock()
		newUpdateTime := time.Now()
		if rdsAuth.updateTime.Add(RDSAuthTokenTTL).After(now) && rdsAuth.token != "" {
			return rdsAuth.token, nil
		}
		peerAWSCredentials := BuildPeerAWSCredentials(rdsAuth.AwsAuthConfig)
		token, err := buildRdsToken(ctx, connConfig, peerAWSCredentials, connectorName)
		if err != nil {
			return "", err
		}
		rdsAuth.token = token
		rdsAuth.updateTime = newUpdateTime
		return token, nil
	}()
}

func buildRdsToken(
	ctx context.Context,
	connConfig RDSConnectionConfig,
	peerAWSCredentials PeerAWSCredentials,
	connectorName string,
) (string, error) {
	awsCredentialsProvider, err := GetAWSCredentialsProvider(ctx, connectorName, peerAWSCredentials)
	if err != nil {
		return "", fmt.Errorf("failed to get AWS credentials provider: %w", err)
	}
	endpoint := fmt.Sprintf("%s:%d", connConfig.Host, connConfig.Port)
	matches := regionRegex.FindStringSubmatch(connConfig.Host)
	if len(matches) < 2 {
		return "", fmt.Errorf("failed to extract region from endpoint %s", connConfig.Host)
	}
	region := matches[1]
	token, err := auth.BuildAuthToken(ctx, endpoint, region, connConfig.User, awsCredentialsProvider.GetUnderlyingProvider())
	if err != nil {
		return "", err
	}
	return token, nil
}
