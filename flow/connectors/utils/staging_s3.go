package utils

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	"github.com/PeerDB-io/peerdb/flow/internal"
	peerdb_clickhouse "github.com/PeerDB-io/peerdb/flow/pkg/clickhouse"
)

// S3StagingStore implements StagingStore for AWS S3 (and S3-compatible services).
type S3StagingStore struct {
	bucket   string
	prefix   string
	fullPath string // original "s3://bucket/prefix" for logging
	creds    AWSCredentialsProvider
}

func NewS3StagingStore(bucketPath string, creds AWSCredentialsProvider) (*S3StagingStore, error) {
	s3o, err := NewS3BucketAndPrefix(bucketPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse S3 bucket path: %w", err)
	}
	return &S3StagingStore{
		bucket:   s3o.Bucket,
		prefix:   s3o.Prefix,
		fullPath: bucketPath,
		creds:    creds,
	}, nil
}

func (s *S3StagingStore) Upload(ctx context.Context, env map[string]string, key string, body io.Reader) error {
	logger := internal.LoggerFromCtx(ctx)

	s3svc, err := CreateS3Client(ctx, s.creds)
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	partSize, err := internal.PeerDBS3PartSize(ctx, env)
	if err != nil {
		return fmt.Errorf("could not get s3 part size config: %w", err)
	}

	uploader := manager.NewUploader(s3svc, func(u *manager.Uploader) {
		if partSize > 0 {
			u.PartSize = partSize
			if partSize > 256*1024*1024 {
				u.Concurrency = 1
			}
		}
	})

	if _, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   body,
	}); err != nil {
		s3Path := "s3://" + s.bucket + "/" + key
		logger.Error("failed to upload file", slog.Any("error", err), slog.String("s3_path", s3Path))
		return fmt.Errorf("failed to upload file to S3: %w", err)
	}

	logger.Info("finished S3 upload", slog.String("key", key))
	return nil
}

func (s *S3StagingStore) TableFunctionExpr(ctx context.Context, key string, format string) (string, error) {
	endpoint := s.creds.GetEndpointURL()
	region := s.creds.GetRegion()
	fileURL := FileURLForS3Service(endpoint, region, s.bucket, key)

	creds, err := s.creds.Retrieve(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to retrieve S3 credentials: %w", err)
	}
	if creds.AWS.CanExpire {
		logger := internal.LoggerFromCtx(ctx)
		logger.Info("Retrieved temporary AWS credentials for table function",
			slog.Time("expiryTimestamp", creds.AWS.Expires),
			slog.Duration("duration", time.Until(creds.AWS.Expires)))
	}

	var expr strings.Builder
	expr.WriteString("s3(")
	expr.WriteString(peerdb_clickhouse.QuoteLiteral(fileURL))
	expr.WriteByte(',')
	expr.WriteString(peerdb_clickhouse.QuoteLiteral(creds.AWS.AccessKeyID))
	expr.WriteByte(',')
	expr.WriteString(peerdb_clickhouse.QuoteLiteral(creds.AWS.SecretAccessKey))
	if creds.AWS.SessionToken != "" {
		expr.WriteByte(',')
		expr.WriteString(peerdb_clickhouse.QuoteLiteral(creds.AWS.SessionToken))
	}
	expr.WriteString(",")
	expr.WriteString(peerdb_clickhouse.QuoteLiteral(format))
	expr.WriteByte(')')
	return expr.String(), nil
}

func (s *S3StagingStore) DeletePrefix(ctx context.Context, prefix string) error {
	logger := internal.LoggerFromCtx(ctx)

	s3svc, err := CreateS3Client(ctx, s.creds)
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	logger.Info("Deleting objects from S3",
		slog.String("bucket", s.bucket), slog.String("prefix", prefix))

	pages := s3.NewListObjectsV2Paginator(s3svc, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to list objects from bucket: %w", err)
		}

		for _, object := range page.Contents {
			if _, err = s3svc.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(s.bucket),
				Key:    object.Key,
			}); err != nil {
				return fmt.Errorf("failed to delete object from bucket: %w", err)
			}
		}
	}

	logger.Info("Deleted objects from S3",
		slog.String("bucket", s.bucket), slog.String("prefix", prefix))
	return nil
}

func (s *S3StagingStore) Validate(ctx context.Context) error {
	s3Client, err := CreateS3Client(ctx, s.creds)
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}
	return PutAndRemoveS3(ctx, s3Client, s.bucket, s.prefix)
}

func (s *S3StagingStore) BucketPath() string {
	return s.fullPath
}

func (s *S3StagingStore) KeyPrefix() string {
	return s.prefix
}

// CredentialsProvider returns the underlying AWS credentials provider.
// This is needed for backward compatibility (e.g., ClickHouse version checks
// that inspect whether credentials use session tokens).
func (s *S3StagingStore) CredentialsProvider() AWSCredentialsProvider {
	return s.creds
}

// Bucket returns the bucket name.
func (s *S3StagingStore) Bucket() string {
	return s.bucket
}

// Prefix returns the key prefix within the bucket.
func (s *S3StagingStore) Prefix() string {
	return s.prefix
}

func newValidationCheckKey(prefix string) string {
	return strings.TrimPrefix(prefix+"/"+_peerDBCheck+uuid.NewString(), "/")
}
