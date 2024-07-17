package connclickhouse

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/aws/aws-sdk-go-v2/aws"
	"go.temporal.io/sdk/log"
	"golang.org/x/mod/semver"

	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

type ClickhouseConnector struct {
	*metadataStore.PostgresMetadata
	database           clickhouse.Conn
	tableSchemaMapping map[string]*protos.TableSchema
	logger             log.Logger
	config             *protos.ClickhouseConfig
	credsProvider      *utils.ClickHouseS3Credentials
	s3Stage            *ClickHouseS3Stage
}

func ValidateS3(ctx context.Context, creds *utils.ClickHouseS3Credentials) error {
	// for validation purposes
	s3Client, err := utils.CreateS3Client(ctx, creds.Provider)
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	object, err := utils.NewS3BucketAndPrefix(creds.BucketPath)
	if err != nil {
		return fmt.Errorf("failed to create S3 bucket and prefix: %w", err)
	}

	return utils.PutAndRemoveS3(ctx, s3Client, object.Bucket, object.Prefix)
}

// Creates and drops a dummy table to validate the peer
func (c *ClickhouseConnector) ValidateCheck(ctx context.Context) error {
	validateDummyTableName := "peerdb_validation_" + shared.RandomString(4)
	// create a table
	err := c.database.Exec(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id UInt64) ENGINE = Memory",
		validateDummyTableName+"_temp"))
	if err != nil {
		return fmt.Errorf("failed to create validation table %s: %w", validateDummyTableName, err)
	}

	// add a column
	err = c.database.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN updated_at DateTime64(9) DEFAULT now()",
		validateDummyTableName+"_temp"))
	if err != nil {
		return fmt.Errorf("failed to add column to validation table %s: %w", validateDummyTableName, err)
	}

	// rename the table
	err = c.database.Exec(ctx, fmt.Sprintf("RENAME TABLE %s TO %s",
		validateDummyTableName+"_temp", validateDummyTableName))
	if err != nil {
		return fmt.Errorf("failed to rename validation table %s: %w", validateDummyTableName, err)
	}

	// insert a row
	err = c.database.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, now())", validateDummyTableName))
	if err != nil {
		return fmt.Errorf("failed to insert into validation table %s: %w", validateDummyTableName, err)
	}

	currentTimestamp := time.Now().UTC().Format("2006-01-02 15:04:05")
	// alter update the row
	err = c.database.Exec(ctx, fmt.Sprintf("ALTER TABLE %s UPDATE updated_at = '%s' WHERE id = 1",
		validateDummyTableName, currentTimestamp))
	if err != nil {
		return fmt.Errorf("failed to update validation table %s: %w", validateDummyTableName, err)
	}

	// drop the table
	err = c.database.Exec(ctx, "DROP TABLE IF EXISTS "+validateDummyTableName)
	if err != nil {
		return fmt.Errorf("failed to drop validation table %s: %w", validateDummyTableName, err)
	}

	// validate s3 stage
	validateErr := ValidateS3(ctx, c.credsProvider)
	if validateErr != nil {
		return fmt.Errorf("failed to validate S3 bucket: %w", validateErr)
	}

	return nil
}

func NewClickhouseConnector(
	ctx context.Context,
	config *protos.ClickhouseConfig,
) (*ClickhouseConnector, error) {
	logger := logger.LoggerFromCtx(ctx)
	database, err := Connect(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to Clickhouse peer: %w", err)
	}

	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		logger.Error("failed to create postgres metadata store", "error", err)
		return nil, err
	}

	credentialsProvider, err := utils.GetAWSCredentialsProvider(ctx, "clickhouse", utils.PeerAWSCredentials{
		Credentials: aws.Credentials{
			AccessKeyID:     config.AccessKeyId,
			SecretAccessKey: config.SecretAccessKey,
		},
		EndpointUrl: config.Endpoint,
		Region:      config.Region,
	})
	if err != nil {
		return nil, err
	}

	awsBucketPath := config.S3Path

	if awsBucketPath == "" {
		deploymentUID := peerdbenv.PeerDBDeploymentUID()
		flowName, _ := ctx.Value(shared.FlowNameKey).(string)
		bucketPathSuffix := fmt.Sprintf("%s/%s",
			url.PathEscape(deploymentUID), url.PathEscape(flowName))
		// Fallback: Get S3 credentials from environment
		awsBucketName, err := peerdbenv.PeerDBClickhouseAWSS3BucketName(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get PeerDB Clickhouse Bucket Name: %w", err)
		}
		if awsBucketName == "" {
			return nil, errors.New("PeerDB Clickhouse Bucket Name not set")
		}

		awsBucketPath = fmt.Sprintf("s3://%s/%s", awsBucketName, bucketPathSuffix)
	}
	clickHouseS3CredentialsNew := utils.ClickHouseS3Credentials{
		Provider:   credentialsProvider,
		BucketPath: awsBucketPath,
	}
	credentials, err := credentialsProvider.Retrieve(ctx)
	if err != nil {
		return nil, err
	}
	if credentials.AWS.SessionToken != "" {
		// This is the minimum version of Clickhouse that actually supports session token
		// https://github.com/ClickHouse/ClickHouse/issues/61230
		minSupportedClickhouseVersion := "v24.3.1"
		clickHouseVersionRow := database.QueryRow(ctx, "SELECT version()")
		var clickHouseVersion string
		err := clickHouseVersionRow.Scan(&clickHouseVersion)
		if err != nil {
			return nil, fmt.Errorf("failed to query clickhouse version: %w", err)
		}
		// Ignore everything after patch version and prefix with "v", else semver.Compare will fail
		versionParts := strings.SplitN(clickHouseVersion, ".", 4)
		if len(versionParts) > 3 {
			versionParts = versionParts[:3]
		}
		cleanedClickHouseVersion := "v" + strings.Join(versionParts, ".")
		if semver.Compare(cleanedClickHouseVersion, minSupportedClickhouseVersion) < 0 {
			return nil, fmt.Errorf(
				"provide S3 Transient Stage details explicitly or upgrade to clickhouse version >= %v, current version is %s. %s",
				minSupportedClickhouseVersion, clickHouseVersion,
				"You can also contact PeerDB support for implicit S3 stage setup for older versions of Clickhouse.")
		}
	}

	return &ClickhouseConnector{
		database:           database,
		PostgresMetadata:   pgMetadata,
		tableSchemaMapping: nil,
		config:             config,
		logger:             logger,
		credsProvider:      &clickHouseS3CredentialsNew,
		s3Stage:            NewClickHouseS3Stage(),
	}, nil
}

func Connect(ctx context.Context, config *protos.ClickhouseConfig) (clickhouse.Conn, error) {
	var tlsSetting *tls.Config
	if !config.DisableTls {
		tlsSetting = &tls.Config{MinVersion: tls.VersionTLS13}
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", config.Host, config.Port)},
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: config.User,
			Password: config.Password,
		},
		TLS:         tlsSetting,
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: "peerdb"},
			},
		},
		DialTimeout: 3600 * time.Second,
		ReadTimeout: 3600 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Clickhouse peer: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to ping to Clickhouse peer: %w", err)
	}

	return conn, nil
}

func (c *ClickhouseConnector) Close() error {
	if c != nil {
		err := c.database.Close()
		if err != nil {
			return fmt.Errorf("error while closing connection to Clickhouse peer: %w", err)
		}
	}
	return nil
}

func (c *ClickhouseConnector) ConnectionActive(ctx context.Context) error {
	// This also checks if database exists
	return c.database.Ping(ctx)
}

func (c *ClickhouseConnector) execWithLogging(ctx context.Context, query string) error {
	c.logger.Info("[clickhouse] executing DDL statement", slog.String("query", query))
	return c.database.Exec(ctx, query)
}
