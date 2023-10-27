package utils

import (
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type AWSSecrets struct {
	AccessKeyID     string
	SecretAccessKey string
	AwsRoleArn      string
	Region          string
	Endpoint        string
}

type S3PeerCredentials struct {
	AccessKeyID     string
	SecretAccessKey string
	AwsRoleArn      string
	Region          string
	Endpoint        string
}

func GetAWSSecrets(creds S3PeerCredentials) (*AWSSecrets, error) {
	awsRegion := creds.Region
	if awsRegion == "" {
		awsRegion = os.Getenv("AWS_REGION")
	}
	if awsRegion == "" {
		return nil, fmt.Errorf("AWS_REGION must be set")
	}

	awsEndpoint := creds.Endpoint
	if awsEndpoint == "" {
		awsEndpoint = os.Getenv("AWS_ENDPOINT")
	}

	awsKey := creds.AccessKeyID
	if awsKey == "" {
		awsKey = os.Getenv("AWS_ACCESS_KEY_ID")
	}

	awsSecret := creds.SecretAccessKey
	if awsSecret == "" {
		awsSecret = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}

	awsRoleArn := creds.AwsRoleArn
	if awsRoleArn == "" {
		awsRoleArn = os.Getenv("AWS_ROLE_ARN")
	}

	// one of (awsKey and awsSecret) or awsRoleArn must be set
	if awsKey == "" && awsSecret == "" && awsRoleArn == "" {
		return nil, fmt.Errorf("one of (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY) or AWS_ROLE_ARN must be set")
	}

	return &AWSSecrets{
		AccessKeyID:     awsKey,
		SecretAccessKey: awsSecret,
		AwsRoleArn:      awsRoleArn,
		Region:          awsRegion,
		Endpoint:        awsEndpoint,
	}, nil
}

type S3BucketAndPrefix struct {
	Bucket string
	Prefix string
}

// path would be something like s3://bucket/prefix
func NewS3BucketAndPrefix(s3Path string) (*S3BucketAndPrefix, error) {
	// Remove s3:// prefix
	stagingPath := strings.TrimPrefix(s3Path, "s3://")

	// Split into bucket and prefix
	splitPath := strings.SplitN(stagingPath, "/", 2)

	bucket := splitPath[0]
	prefix := ""
	if len(splitPath) > 1 {
		// Remove leading and trailing slashes from prefix
		prefix = strings.Trim(splitPath[1], "/")
	}

	return &S3BucketAndPrefix{
		Bucket: bucket,
		Prefix: prefix,
	}, nil
}

func CreateS3Client(s3Creds S3PeerCredentials) (*s3.S3, error) {
	awsSecrets, err := GetAWSSecrets(s3Creds)
	if err != nil {
		return nil, fmt.Errorf("failed to get AWS secrets: %w", err)
	}

	sess := session.Must(session.NewSession(&aws.Config{
		Region:   aws.String(awsSecrets.Region),
		Endpoint: aws.String(awsSecrets.Endpoint),
	}))

	s3svc := s3.New(sess)
	return s3svc, nil
}
