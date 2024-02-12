package connclickhouse

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	conns3 "github.com/PeerDB-io/peer-flow/connectors/s3"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
)

type ClickhouseConnector struct {
	database           *sql.DB
	pgMetadata         *metadataStore.PostgresMetadataStore
	tableSchemaMapping map[string]*protos.TableSchema
	logger             log.Logger
	config             *protos.ClickhouseConfig
	creds              utils.S3PeerCredentials
}

func ValidateS3(ctx context.Context, bucketUrl string, creds utils.S3PeerCredentials) error {
	// for validation purposes
	s3Client, err := utils.CreateS3Client(creds)
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	validErr := conns3.ValidCheck(ctx, s3Client, bucketUrl, nil)
	if validErr != nil {
		return validErr
	}

	return nil
}

func NewClickhouseConnector(
	ctx context.Context,
	config *protos.ClickhouseConfig,
) (*ClickhouseConnector, error) {
	logger := logger.LoggerFromCtx(ctx)
	database, err := connect(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to Clickhouse peer: %w", err)
	}

	pgMetadata, err := metadataStore.NewPostgresMetadataStore(ctx)
	if err != nil {
		logger.Error("failed to create postgres metadata store", "error", err)
		return nil, err
	}

	s3PeerCreds := utils.S3PeerCredentials{
		AccessKeyID:     config.AccessKeyId,
		SecretAccessKey: config.SecretAccessKey,
		Region:          config.Region,
	}

	validateErr := ValidateS3(ctx, config.S3Path, s3PeerCreds)
	if validateErr != nil {
		return nil, fmt.Errorf("failed to validate S3 bucket: %w", validateErr)
	}

	return &ClickhouseConnector{
		database:           database,
		pgMetadata:         pgMetadata,
		tableSchemaMapping: nil,
		config:             config,
		creds:              s3PeerCreds,
		logger:             logger,
	}, nil
}

func connect(ctx context.Context, config *protos.ClickhouseConfig) (*sql.DB, error) {
	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", config.Host, config.Port)},
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: config.User,
			Password: config.Password,
		},
		TLS: &tls.Config{
			InsecureSkipVerify: true,
		},
	})

	if err := conn.PingContext(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to ping to Clickhouse peer: %w", err)
	}

	return conn, nil
}

func (c *ClickhouseConnector) Close(_ context.Context) error {
	if c == nil || c.database == nil {
		return nil
	}

	err := c.database.Close()
	if err != nil {
		return fmt.Errorf("error while closing connection to Clickhouse peer: %w", err)
	}
	return nil
}

func (c *ClickhouseConnector) ConnectionActive(ctx context.Context) error {
	if c == nil || c.database == nil {
		return fmt.Errorf("ClickhouseConnector is nil")
	}

	// This also checks if database exists
	err := c.database.PingContext(ctx)
	return err
}
