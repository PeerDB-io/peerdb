package utils

import (
	"fmt"
	"os"
	"strings"
)

type AWSSecrets struct {
	AccessKeyID     string
	SecretAccessKey string
	Region          string
}

func GetAWSSecrets() (*AWSSecrets, error) {
	awsKey := os.Getenv("AWS_ACCESS_KEY_ID")
	if awsKey == "" {
		return nil, fmt.Errorf("AWS_ACCESS_KEY_ID must be set")
	}

	awsSecret := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if awsSecret == "" {
		return nil, fmt.Errorf("AWS_SECRET_ACCESS_KEY must be set")
	}

	awsRegion := os.Getenv("AWS_REGION")
	if awsRegion == "" {
		return nil, fmt.Errorf("AWS_REGION must be set")
	}

	return &AWSSecrets{
		AccessKeyID:     awsKey,
		SecretAccessKey: awsSecret,
		Region:          awsRegion,
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
