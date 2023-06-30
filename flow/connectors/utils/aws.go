package utils

import (
	"fmt"
	"os"
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
