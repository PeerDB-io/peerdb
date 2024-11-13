package utils

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyendpoints "github.com/aws/smithy-go/endpoints"
	"github.com/google/uuid"

	"github.com/PeerDB-io/peer-flow/shared"
)

const (
	_peerDBCheck = "peerdb_check"
)

var s3CompatibleServiceEndpointPattern = regexp.MustCompile(`^https?://[a-zA-Z0-9.-]+(:\d+)?$`)

type AWSSecrets struct {
	AccessKeyID     string
	SecretAccessKey string
	AwsRoleArn      string
	Region          string
	Endpoint        string
	SessionToken    string
}

type PeerAWSCredentials struct {
	Credentials aws.Credentials
	RoleArn     *string
	EndpointUrl *string
	Region      string
}

type S3PeerCredentials struct {
	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
	AwsRoleArn      string `json:"awsRoleArn"`
	SessionToken    string `json:"sessionToken"`
	Region          string `json:"region"`
	Endpoint        string `json:"endpoint"`
}

type ClickHouseS3Credentials struct {
	Provider   AWSCredentialsProvider
	BucketPath string
}

type AWSCredentials struct {
	EndpointUrl *string
	AWS         aws.Credentials
}

type AWSCredentialsProvider interface {
	Retrieve(ctx context.Context) (AWSCredentials, error)
	GetUnderlyingProvider() aws.CredentialsProvider
	GetRegion() string
	GetEndpointURL() string
}

type ConfigBasedAWSCredentialsProvider struct {
	config aws.Config
}

func (r *ConfigBasedAWSCredentialsProvider) GetUnderlyingProvider() aws.CredentialsProvider {
	return r.config.Credentials
}

func (r *ConfigBasedAWSCredentialsProvider) GetRegion() string {
	return r.config.Region
}

func (r *ConfigBasedAWSCredentialsProvider) GetEndpointURL() string {
	endpoint := ""
	if r.config.BaseEndpoint != nil {
		endpoint = *r.config.BaseEndpoint
	}

	return endpoint
}

// Retrieve should be called as late as possible in order to have credentials with latest expiry
func (r *ConfigBasedAWSCredentialsProvider) Retrieve(ctx context.Context) (AWSCredentials, error) {
	retrieved, err := r.config.Credentials.Retrieve(ctx)
	if err != nil {
		return AWSCredentials{}, err
	}
	return AWSCredentials{
		AWS:         retrieved,
		EndpointUrl: r.config.BaseEndpoint,
	}, nil
}

func NewConfigBasedAWSCredentialsProvider(config aws.Config) AWSCredentialsProvider {
	return &ConfigBasedAWSCredentialsProvider{config: config}
}

type StaticAWSCredentialsProvider struct {
	credentials AWSCredentials
	region      string
}

func (s *StaticAWSCredentialsProvider) GetUnderlyingProvider() aws.CredentialsProvider {
	return credentials.NewStaticCredentialsProvider(s.credentials.AWS.AccessKeyID, s.credentials.AWS.SecretAccessKey,
		s.credentials.AWS.SessionToken)
}

func (s *StaticAWSCredentialsProvider) GetRegion() string {
	return s.region
}

func (s *StaticAWSCredentialsProvider) Retrieve(ctx context.Context) (AWSCredentials, error) {
	return s.credentials, nil
}

func (s *StaticAWSCredentialsProvider) GetEndpointURL() string {
	if s.credentials.EndpointUrl != nil {
		return *s.credentials.EndpointUrl
	}
	return ""
}

func NewStaticAWSCredentialsProvider(credentials AWSCredentials, region string) AWSCredentialsProvider {
	return &StaticAWSCredentialsProvider{
		credentials: credentials,
		region:      region,
	}
}

func getPeerDBAWSEnv(connectorName string, awsKey string) string {
	return os.Getenv(fmt.Sprintf("PEERDB_%s_AWS_CREDENTIALS_%s", strings.ToUpper(connectorName), awsKey))
}

func LoadPeerDBAWSEnvConfigProvider(connectorName string) AWSCredentialsProvider {
	accessKeyId := getPeerDBAWSEnv(connectorName, "AWS_ACCESS_KEY_ID")
	secretAccessKey := getPeerDBAWSEnv(connectorName, "AWS_SECRET_ACCESS_KEY")
	region := getPeerDBAWSEnv(connectorName, "AWS_REGION")
	endpointUrl := getPeerDBAWSEnv(connectorName, "AWS_ENDPOINT_URL_S3")
	var endpointUrlPtr *string
	if endpointUrl != "" {
		endpointUrlPtr = &endpointUrl
	}

	if accessKeyId == "" && secretAccessKey == "" && region == "" && endpointUrl == "" {
		return nil
	}

	return NewStaticAWSCredentialsProvider(AWSCredentials{
		AWS: aws.Credentials{
			AccessKeyID:     accessKeyId,
			SecretAccessKey: secretAccessKey,
		},
		EndpointUrl: endpointUrlPtr,
	}, region)
}

func GetAWSCredentialsProvider(ctx context.Context, connectorName string, peerCredentials PeerAWSCredentials) (AWSCredentialsProvider, error) {
	if !(peerCredentials.Credentials.AccessKeyID == "" && peerCredentials.Credentials.SecretAccessKey == "" &&
		peerCredentials.Region == "" && (peerCredentials.RoleArn == nil || *peerCredentials.RoleArn == "") &&
		(peerCredentials.EndpointUrl == nil || *peerCredentials.EndpointUrl == "")) {
		staticProvider := NewStaticAWSCredentialsProvider(AWSCredentials{
			AWS:         peerCredentials.Credentials,
			EndpointUrl: peerCredentials.EndpointUrl,
		}, peerCredentials.Region)
		if peerCredentials.RoleArn == nil || *peerCredentials.RoleArn == "" {
			shared.LoggerFromCtx(ctx).Info("Received AWS credentials from peer for connector: " + connectorName)
			return staticProvider, nil
		}
		awsConfig, err := config.LoadDefaultConfig(ctx, func(options *config.LoadOptions) error {
			options.AssumeRoleCredentialOptions = func(assumeOptions *stscreds.AssumeRoleOptions) {
				assumeOptions.RoleARN = *peerCredentials.RoleArn
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		shared.LoggerFromCtx(ctx).Info("Received AWS credentials with role from peer for connector: " + connectorName)
		return NewConfigBasedAWSCredentialsProvider(awsConfig), nil
	}
	envCredentialsProvider := LoadPeerDBAWSEnvConfigProvider(connectorName)
	if envCredentialsProvider != nil {
		shared.LoggerFromCtx(ctx).Info("Received AWS credentials from PeerDB Env for connector: " + connectorName)
		return envCredentialsProvider, nil
	}

	awsConfig, err := config.LoadDefaultConfig(ctx, func(options *config.LoadOptions) error {
		return nil
	})
	if err != nil {
		return nil, err
	}
	shared.LoggerFromCtx(ctx).Info("Received AWS credentials from SDK config for connector: " + connectorName)
	return NewConfigBasedAWSCredentialsProvider(awsConfig), nil
}

func FileURLForS3Service(endpoint string, region string, bucket string, filePath string) string {
	if s3CompatibleServiceEndpointPattern.MatchString(endpoint) {
		return fmt.Sprintf("%s/%s/%s", endpoint, bucket, filePath)
	}
	return fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s", bucket, region, filePath)
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

type resolverV2 struct {
	url.URL
}

func (r *resolverV2) ResolveEndpoint(ctx context.Context, params s3.EndpointParameters) (
	smithyendpoints.Endpoint, error,
) {
	uri := r.URL
	uri.Path += "/" + *params.Bucket
	return smithyendpoints.Endpoint{
		URI: uri,
	}, nil
}

func CreateS3Client(ctx context.Context, credsProvider AWSCredentialsProvider) (*s3.Client, error) {
	awsCredentials, err := credsProvider.Retrieve(ctx)
	if err != nil {
		return nil, err
	}

	options := s3.Options{
		Region:      credsProvider.GetRegion(),
		Credentials: credsProvider.GetUnderlyingProvider(),
	}
	if awsCredentials.EndpointUrl != nil && *awsCredentials.EndpointUrl != "" {
		options.BaseEndpoint = awsCredentials.EndpointUrl
		options.UsePathStyle = true
		url, err := url.Parse(*awsCredentials.EndpointUrl)
		if err != nil {
			return nil, err
		}
		options.EndpointResolverV2 = &resolverV2{
			URL: *url,
		}

		if strings.Contains(*awsCredentials.EndpointUrl, "storage.googleapis.com") {
			// Assign custom client with our own transport
			options.HTTPClient = &http.Client{
				Transport: &RecalculateV4Signature{
					next:        http.DefaultTransport,
					signer:      v4.NewSigner(),
					credentials: credsProvider.GetUnderlyingProvider(),
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
	if err := lt.signer.SignHTTP(req.Context(), creds, req, v4.GetPayloadHash(req.Context()), "s3", lt.region, timeDate); err != nil {
		return nil, err
	}
	// Reset Accept-Encoding if desired
	req.Header.Set("Accept-Encoding", acceptEncodingValue)

	// follows up the original round tripper
	return lt.next.RoundTrip(req)
}

// Write an empty file and then delete it
// to check if we have write permissions
func PutAndRemoveS3(ctx context.Context, client *s3.Client, bucket string, prefix string) error {
	reader := strings.NewReader(time.Now().Format(time.RFC3339))
	bucketName := aws.String(bucket)
	temporaryObjectPath := prefix + "/" + _peerDBCheck + uuid.New().String()
	key := aws.String(strings.TrimPrefix(temporaryObjectPath, "/"))

	if _, putErr := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: bucketName,
		Key:    key,
		Body:   reader,
	}); putErr != nil {
		return fmt.Errorf("failed to write to bucket: %w", putErr)
	}

	if _, delErr := client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: bucketName,
		Key:    key,
	}); delErr != nil {
		return fmt.Errorf("failed to delete from bucket: %w", delErr)
	}

	return nil
}
