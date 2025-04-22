package utils

import (
	"context"
	"fmt"
	"regexp"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

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

func GetRdsToken(ctx context.Context, connConfig *pgx.ConnConfig, peerAWSCredentials PeerAWSCredentials, connectorName string) (string, error) {
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
