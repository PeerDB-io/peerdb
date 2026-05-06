package connclickhouse

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

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/internal"
	peerdb_clickhouse "github.com/PeerDB-io/peerdb/flow/pkg/clickhouse"
)

// s3StagingStore implements StagingStore for AWS S3 (and S3-compatible services).
type s3StagingStore struct {
	creds    utils.AWSCredentialsProvider
	bucket   string
	prefix   string
	fullPath string // original "s3://bucket/prefix" for logging
}

func newS3StagingStore(bucketPath string, creds utils.AWSCredentialsProvider) (*s3StagingStore, error) {
	s3o, err := utils.NewS3BucketAndPrefix(bucketPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse S3 bucket path: %w", err)
	}
	return &s3StagingStore{
		bucket:   s3o.Bucket,
		prefix:   s3o.Prefix,
		fullPath: bucketPath,
		creds:    creds,
	}, nil
}

func (s *s3StagingStore) Upload(ctx context.Context, env map[string]string, key string, body io.Reader) error {
	logger := internal.LoggerFromCtx(ctx)

	s3svc, err := utils.CreateS3Client(ctx, s.creds)
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

func (s *s3StagingStore) TableFunctionExpr(ctx context.Context, key string, format string) (string, error) {
	endpoint := s.creds.GetEndpointURL()
	region := s.creds.GetRegion()
	fileURL := utils.FileURLForS3Service(endpoint, region, s.bucket, key)

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

func (s *s3StagingStore) DeletePrefix(ctx context.Context, prefix string) error {
	logger := internal.LoggerFromCtx(ctx)

	s3svc, err := utils.CreateS3Client(ctx, s.creds)
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

func (s *s3StagingStore) Validate(ctx context.Context) error {
	s3Client, err := utils.CreateS3Client(ctx, s.creds)
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}
	return utils.PutAndRemoveS3(ctx, s3Client, s.bucket, s.prefix)
}

func (s *s3StagingStore) BucketPath() string {
	return s.fullPath
}

func (s *s3StagingStore) KeyPrefix() string {
	return s.prefix
}
