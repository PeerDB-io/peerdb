package utils

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type AWSSecrets struct {
	AccessKeyID     string
	SecretAccessKey string
	AwsRoleArn      string
	Region          string
	Endpoint        string
}

type S3PeerCredentials struct {
	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
	AwsRoleArn      string `json:"awsRoleArn"`
	Region          string `json:"region"`
	Endpoint        string `json:"endpoint"`
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
	bucket, prefix, _ := strings.Cut(stagingPath, "/")

	return &S3BucketAndPrefix{
		Bucket: bucket,
		Prefix: strings.Trim(prefix, "/"),
	}, nil
}

func CreateS3Client(s3Creds S3PeerCredentials) (*s3.Client, error) {
	awsSecrets, err := GetAWSSecrets(s3Creds)
	if err != nil {
		return nil, fmt.Errorf("failed to get AWS secrets: %w", err)
	}
	options := s3.Options{
		Region:      awsSecrets.Region,
		Credentials: credentials.NewStaticCredentialsProvider(awsSecrets.AccessKeyID, awsSecrets.SecretAccessKey, ""),
	}
	if awsSecrets.Endpoint != "" {
		options.BaseEndpoint = &awsSecrets.Endpoint
		if strings.Contains(awsSecrets.Endpoint, "storage.googleapis.com") {
			// Assign custom client with our own transport
			options.HTTPClient = &http.Client{
				Transport: &RecalculateV4Signature{
					next:        http.DefaultTransport,
					signer:      v4.NewSigner(),
					credentials: options.Credentials,
					region:      options.Region,
				},
			}
		}
	}

	return s3.New(options), nil
}

// RecalculateV4Signature allow GCS over S3, removing Accept-Encoding header from sign
// https://stackoverflow.com/a/74382598/1204665
// https://github.com/aws/aws-sdk-go-v2/issues/1816
type RecalculateV4Signature struct {
	next        http.RoundTripper
	signer      *v4.Signer
	credentials aws.CredentialsProvider
	region      string
}

func (lt *RecalculateV4Signature) RoundTrip(req *http.Request) (*http.Response, error) {
	// store for later use
	acceptEncodingValue := req.Header.Get("Accept-Encoding")

	// delete the header so the header doesn't account for in the signature
	req.Header.Del("Accept-Encoding")

	// sign with the same date
	timeString := req.Header.Get("X-Amz-Date")
	timeDate, _ := time.Parse("20060102T150405Z", timeString)

	creds, err := lt.credentials.Retrieve(req.Context())
	if err != nil {
		return nil, err
	}
	err = lt.signer.SignHTTP(req.Context(), creds, req, v4.GetPayloadHash(req.Context()), "s3", lt.region, timeDate)
	if err != nil {
		return nil, err
	}
	// Reset Accept-Encoding if desired
	req.Header.Set("Accept-Encoding", acceptEncodingValue)

	// follows up the original round tripper
	return lt.next.RoundTrip(req)
}
