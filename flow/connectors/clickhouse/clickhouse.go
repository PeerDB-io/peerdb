package connclickhouse

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"slices"
	"strings"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/aws/aws-sdk-go-v2/aws"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	peerdb_clickhouse "github.com/PeerDB-io/peerdb/flow/pkg/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type ClickHouseConnector struct {
	*metadataStore.PostgresMetadata
	database      clickhouse.Conn
	logger        log.Logger
	Config        *protos.ClickhouseConfig
	credsProvider *utils.ClickHouseS3Credentials
	chVersion     *clickhouseproto.Version
}

func NewClickHouseConnector(
	ctx context.Context,
	env map[string]string,
	config *protos.ClickhouseConfig,
) (*ClickHouseConnector, error) {
	logger := internal.LoggerFromCtx(ctx)
	database, err := Connect(ctx, env, config)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to ClickHouse peer: %w", err)
	}

	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		logger.Error("failed to create postgres metadata store", slog.Any("error", err))
		return nil, err
	}

	var awsConfig utils.PeerAWSCredentials
	var awsBucketPath string
	if config.S3 != nil {
		awsConfig = utils.NewPeerAWSCredentials(config.S3)
		awsBucketPath = config.S3.Url
	} else {
		awsConfig = utils.PeerAWSCredentials{
			Credentials: aws.Credentials{
				AccessKeyID:     config.AccessKeyId,
				SecretAccessKey: config.SecretAccessKey,
			},
			EndpointUrl: config.Endpoint,
			Region:      config.Region,
		}
		awsBucketPath = config.S3Path
	}

	credentialsProvider, err := utils.GetAWSCredentialsProvider(ctx, "clickhouse", awsConfig)
	if err != nil {
		return nil, err
	}

	if awsBucketPath == "" {
		deploymentUID := internal.PeerDBDeploymentUID()
		flowName, _ := ctx.Value(shared.FlowNameKey).(string)
		bucketPathSuffix := fmt.Sprintf("%s/%s", url.PathEscape(deploymentUID), url.PathEscape(flowName))
		// Fallback: Get S3 credentials from environment
		awsBucketName, err := internal.PeerDBClickHouseAWSS3BucketName(ctx, env)
		if err != nil {
			return nil, fmt.Errorf("failed to get PeerDB ClickHouse Bucket Name: %w", err)
		}
		if awsBucketName == "" {
			return nil, errors.New("PeerDB ClickHouse Bucket Name not set")
		}

		awsBucketPath = fmt.Sprintf("s3://%s/%s", awsBucketName, bucketPathSuffix)
	}

	credentials, err := credentialsProvider.Retrieve(ctx)
	if err != nil {
		return nil, err
	}

	clickHouseVersion, err := database.ServerVersion()
	if err != nil {
		return nil, fmt.Errorf("failed to get ClickHouse version: %w", err)
	}
	connector := &ClickHouseConnector{
		database:         database,
		PostgresMetadata: pgMetadata,
		Config:           config,
		logger:           logger,
		credsProvider: &utils.ClickHouseS3Credentials{
			Provider:   credentialsProvider,
			BucketPath: awsBucketPath,
		},
		chVersion: &clickHouseVersion.Version,
	}

	if credentials.AWS.SessionToken != "" {
		// 24.3.1 is minimum version of ClickHouse that actually supports session token
		// https://github.com/ClickHouse/ClickHouse/issues/61230
		if !clickhouseproto.CheckMinVersion(
			clickhouseproto.Version{Major: 24, Minor: 3, Patch: 1},
			clickHouseVersion.Version,
		) {
			return nil, fmt.Errorf(
				"provide S3 Transient Stage details explicitly or upgrade to ClickHouse version >= 24.3.1, current version is %s. %s",
				clickHouseVersion,
				"You can also contact PeerDB support for implicit S3 stage setup for older versions of ClickHouse.")
		}
	}

	return connector, nil
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

func ValidateClickHouseHost(ctx context.Context, chHost string, allowedDomainString string) error {
	allowedDomains := strings.Split(allowedDomainString, ",")
	if len(allowedDomains) == 0 {
		return nil
	}
	// check if chHost ends with one of the allowed domains
	for _, domain := range allowedDomains {
		if strings.HasSuffix(chHost, domain) {
			return nil
		}
	}
	return fmt.Errorf("invalid ClickHouse host domain: %s. Allowed domains: %s",
		chHost, strings.Join(allowedDomains, ","))
}

// Performs some checks on the ClickHouse peer to ensure it will work for mirrors
func (c *ClickHouseConnector) ValidateCheck(ctx context.Context) error {
	// validate clickhouse host
	allowedDomains := internal.PeerDBClickHouseAllowedDomains()
	if err := ValidateClickHouseHost(ctx, c.Config.Host, allowedDomains); err != nil {
		return err
	}
	validateDummyTableName := "peerdb_validation_" + shared.RandomString(4)
	validateDummyTableNameRenamed := validateDummyTableName + "_renamed"
	// create a table
	if err := c.exec(ctx,
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id UInt64) ENGINE = ReplacingMergeTree ORDER BY id;`, validateDummyTableName),
	); err != nil {
		return fmt.Errorf("failed to create validation table %s: %w", validateDummyTableName, err)
	}
	defer func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		for _, table := range []string{validateDummyTableName, validateDummyTableNameRenamed} {
			for attempt := range 3 {
				if attempt > 0 {
					time.Sleep(time.Duration(attempt) * 2 * time.Second)
				}
				err := c.exec(dropCtx, "DROP TABLE IF EXISTS "+table)
				if err == nil {
					break
				}
				var chException *clickhouse.Exception
				if errors.As(err, &chException) && chproto.Error(chException.Code) == chproto.ErrUnfinished {
					c.logger.Warn("validation drop table blocked by in-flight DDL, retrying",
						slog.String("table", table), slog.Int("attempt", attempt+1))
					continue
				}
				c.logger.Error("validation failed to drop table", slog.String("table", table), slog.Any("error", err))
				break
			}
		}
	}()

	// add a column
	if err := c.exec(ctx,
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN updated_at DateTime64(9) DEFAULT now64()", validateDummyTableName),
	); err != nil {
		return fmt.Errorf("failed to add column to validation table %s: %w", validateDummyTableName, err)
	}

	// rename the table
	if err := c.exec(ctx,
		fmt.Sprintf("RENAME TABLE %s TO %s", validateDummyTableName, validateDummyTableNameRenamed),
	); err != nil {
		return fmt.Errorf("failed to rename validation table %s: %w", validateDummyTableName, err)
	}

	// insert a row
	if err := c.exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, now64())", validateDummyTableNameRenamed)); err != nil {
		return fmt.Errorf("failed to insert into validation table %s: %w", validateDummyTableNameRenamed, err)
	}

	// drop the table
	if err := c.exec(ctx, "DROP TABLE IF EXISTS "+validateDummyTableNameRenamed); err != nil {
		return fmt.Errorf("failed to drop validation table %s: %w", validateDummyTableNameRenamed, err)
	}

	// validate s3 stage
	if err := ValidateS3(ctx, c.credsProvider); err != nil {
		return fmt.Errorf("failed to validate S3 bucket: %w", err)
	}

	return nil
}

func Connect(ctx context.Context, env map[string]string, config *protos.ClickhouseConfig) (clickhouse.Conn, error) {
	var tlsSetting *tls.Config
	if !config.DisableTls {
		tlsSetting = &tls.Config{MinVersion: tls.VersionTLS13}
		if config.Certificate != nil || config.PrivateKey != nil {
			if config.Certificate == nil || config.PrivateKey == nil {
				return nil, errors.New("both certificate and private key must be provided if using certificate-based authentication")
			}
			cert, err := tls.X509KeyPair([]byte(*config.Certificate), []byte(*config.PrivateKey))
			if err != nil {
				return nil, fmt.Errorf("failed to parse provided certificate: %w", err)
			}
			tlsSetting.Certificates = []tls.Certificate{cert}
		}
		if config.RootCa != nil {
			caPool := x509.NewCertPool()
			if !caPool.AppendCertsFromPEM([]byte(*config.RootCa)) {
				return nil, errors.New("failed to parse provided root CA")
			}
			tlsSetting.RootCAs = caPool
		}
		if config.TlsHost != "" {
			tlsSetting.ServerName = config.TlsHost
		}
	}

	settings := clickhouse.Settings{
		// See: https://clickhouse.com/docs/en/cloud/reference/shared-merge-tree#consistency
		"select_sequential_consistency": uint64(1),
		// broken downstream views should not interrupt ingestion
		"ignore_materialized_views_with_dropped_target_table": true,
		// avoid "there is no metadata of table ..."
		"alter_sync": uint64(1),
		// to handle JSON like "{"key": []}"
		"input_format_json_infer_incomplete_types_as_strings": uint64(1),
	}
	if maxInsertThreads, err := internal.PeerDBClickHouseMaxInsertThreads(ctx, env); err != nil {
		return nil, fmt.Errorf("failed to load max_insert_threads config: %w", err)
	} else if maxInsertThreads != 0 {
		settings["max_insert_threads"] = maxInsertThreads
	}
	if config.Cluster != "" {
		settings["insert_distributed_sync"] = uint64(1)
	}
	clientName, err := internal.PeerDBClickHouseClientName(ctx, env)
	if err != nil {
		return nil, fmt.Errorf("failed to load ClickHouse client name: %w", err)
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{shared.JoinHostPort(config.Host, config.Port)},
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
				{Name: clientName},
			},
		},
		Settings:    settings,
		DialTimeout: 3600 * time.Second,
		ReadTimeout: 3600 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse peer: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to ping to ClickHouse peer: %w", err)
	}

	return conn, nil
}

func (c *ClickHouseConnector) exec(ctx context.Context, query string) error {
	return peerdb_clickhouse.Exec(ctx, c.logger, c.database, query)
}

func (c *ClickHouseConnector) execWithConnection(ctx context.Context, conn clickhouse.Conn, query string) error {
	return peerdb_clickhouse.Exec(ctx, c.logger, conn, query)
}

func (c *ClickHouseConnector) query(ctx context.Context, query string) (driver.Rows, error) {
	return peerdb_clickhouse.Query(ctx, c.logger, c.database, query)
}

func (c *ClickHouseConnector) queryRow(ctx context.Context, query string) driver.Row {
	return peerdb_clickhouse.QueryRow(ctx, c.logger, c.database, query)
}

func (c *ClickHouseConnector) Close() error {
	if c != nil {
		if err := c.database.Close(); err != nil {
			return fmt.Errorf("error while closing connection to ClickHouse peer: %w", err)
		}
	}
	return nil
}

func (c *ClickHouseConnector) ConnectionActive(ctx context.Context) error {
	// This also checks if database exists
	return c.database.Ping(ctx)
}

func (c *ClickHouseConnector) execWithLogging(ctx context.Context, query string) error {
	c.logger.Info("[clickhouse] executing DDL statement", slog.String("query", query))
	return c.exec(ctx, query)
}

func (c *ClickHouseConnector) processTableComparison(dstTableName string, srcSchema *protos.TableSchema,
	dstSchema []peerdb_clickhouse.ClickHouseColumn, peerDBColumns []string, tableMapping *protos.TableMapping,
) error {
	for _, srcField := range srcSchema.Columns {
		colName := srcField.Name
		// if the column is mapped to a different name, find and use that name instead
		for _, col := range tableMapping.Columns {
			if col.SourceName == colName {
				if col.DestinationName != "" {
					colName = col.DestinationName
				}
				break
			}
		}
		found := false
		// compare either the source column name or the mapped destination column name to the ClickHouse schema
		for _, dstField := range dstSchema {
			// not doing type checks for now
			if dstField.Name == colName {
				if dstField.DefaultKind == "ALIAS" || dstField.DefaultKind == "MATERIALIZED" {
					return fmt.Errorf("field %s in destination table %s is %s and doesn't support INSERTs",
						srcField.Name, dstTableName, dstField.DefaultKind)
				}

				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("field %s not found in destination table %s", srcField.Name, dstTableName)
		}
	}
	foundPeerDBColumns := 0
	for _, dstField := range dstSchema {
		// all these columns need to be present in the destination table
		if slices.Contains(peerDBColumns, dstField.Name) {
			foundPeerDBColumns++
		}
	}
	if foundPeerDBColumns != len(peerDBColumns) {
		return fmt.Errorf("not all PeerDB columns found in destination table %s", dstTableName)
	}
	return nil
}

func (c *ClickHouseConnector) GetVersion(ctx context.Context) (string, error) {
	if c.chVersion != nil {
		return c.chVersion.String(), nil
	}

	clickhouseVersion, err := c.database.ServerVersion()
	if err != nil {
		return "", fmt.Errorf("failed to get ClickHouse version: %w", err)
	}
	c.logger.Info("[clickhouse] version", slog.String("version", clickhouseVersion.DisplayName))
	return clickhouseVersion.Version.String(), nil
}

func (c *ClickHouseConnector) GetFlags(ctx context.Context) ([]string, error) {
	var flags []string

	var time64Setting string
	err := c.queryRow(ctx,
		"SELECT value FROM system.settings WHERE name = 'enable_time_time64_type'",
	).Scan(&time64Setting)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("failed to query enable_time_time64_type setting: %w", err)
		}
	} else if time64Setting == "1" {
		c.logger.Info("[clickhouse] enable_time_time64_type is enabled")
		flags = append(flags, shared.Flag_ClickHouseTime64Enabled)
	}

	return flags, nil
}

func GetTableSchemaForTable(tm *protos.TableMapping, columns []driver.ColumnType) (*protos.TableSchema, error) {
	colFields := make([]*protos.FieldDescription, 0, len(columns))
	for _, column := range columns {
		if slices.Contains(tm.Exclude, column.Name()) {
			continue
		}

		var qkind types.QValueKind
		switch column.DatabaseTypeName() {
		case "String", "Nullable(String)", "LowCardinality(String)", "LowCardinality(Nullable(String))":
			qkind = types.QValueKindString
		case "Bool", "Nullable(Bool)":
			qkind = types.QValueKindBoolean
		case "Int8", "Nullable(Int8)":
			qkind = types.QValueKindInt8
		case "Int16", "Nullable(Int16)":
			qkind = types.QValueKindInt16
		case "Int32", "Nullable(Int32)":
			qkind = types.QValueKindInt32
		case "Int64", "Nullable(Int64)":
			qkind = types.QValueKindInt64
		case "Int256", "Nullable(Int256)":
			qkind = types.QValueKindInt256
		case "UInt8", "Nullable(UInt8)":
			qkind = types.QValueKindUInt8
		case "UInt16", "Nullable(UInt16)":
			qkind = types.QValueKindUInt16
		case "UInt32", "Nullable(UInt32)":
			qkind = types.QValueKindUInt32
		case "UInt64", "Nullable(UInt64)":
			qkind = types.QValueKindUInt64
		case "UInt256", "Nullable(UInt256)":
			qkind = types.QValueKindUInt256
		case "UUID", "Nullable(UUID)":
			qkind = types.QValueKindUUID
		case "DateTime64(6)", "Nullable(DateTime64(6))", "DateTime64(9)", "Nullable(DateTime64(9))":
			qkind = types.QValueKindTimestamp
		case "Time64(6)", "Nullable(Time64(6))":
			qkind = types.QValueKindTime
		case "Date32", "Nullable(Date32)":
			qkind = types.QValueKindDate
		case "Float32", "Nullable(Float32)":
			qkind = types.QValueKindFloat32
		case "Float64", "Nullable(Float64)":
			qkind = types.QValueKindFloat64
		case "Array(Int32)":
			qkind = types.QValueKindArrayInt32
		case "Array(Float32)":
			qkind = types.QValueKindArrayFloat32
		case "Array(Float64)":
			qkind = types.QValueKindArrayFloat64
		case "Array(String)", "Array(LowCardinality(String))":
			qkind = types.QValueKindArrayString
		case "Array(UUID)":
			qkind = types.QValueKindArrayUUID
		case "Array(DateTime64(6))":
			qkind = types.QValueKindArrayTimestamp
		case "Array(Int64)":
			qkind = types.QValueKindArrayInt64
		case "Array(Bool)":
			qkind = types.QValueKindArrayBoolean
		case "Array(Date)":
			qkind = types.QValueKindArrayDate
		case "JSON":
			qkind = types.QValueKindJSON
		default:
			if strings.Contains(column.DatabaseTypeName(), "Decimal") {
				if strings.HasPrefix(column.DatabaseTypeName(), "Array(") {
					qkind = types.QValueKindArrayNumeric
				} else {
					qkind = types.QValueKindNumeric
				}
			} else {
				return nil, fmt.Errorf("failed to resolve QValueKind for %s", column.DatabaseTypeName())
			}
		}

		colFields = append(colFields, &protos.FieldDescription{
			Name:         column.Name(),
			Type:         string(qkind),
			TypeModifier: -1,
			Nullable:     column.Nullable(),
		})
	}

	return &protos.TableSchema{
		TableIdentifier: tm.SourceTableIdentifier,
		Columns:         colFields,
		System:          protos.TypeSystem_Q,
	}, nil
}

func (c *ClickHouseConnector) GetTableSchema(
	ctx context.Context,
	_env map[string]string,
	_version uint32,
	_system protos.TypeSystem,
	tableMappings []*protos.TableMapping,
) (map[string]*protos.TableSchema, error) {
	res := make(map[string]*protos.TableSchema, len(tableMappings))
	for _, tm := range tableMappings {
		rows, err := c.database.Query(ctx, fmt.Sprintf("select * from %s limit 0", tm.SourceTableIdentifier))
		if err != nil {
			return nil, err
		}

		tableSchema, err := GetTableSchemaForTable(tm, rows.ColumnTypes())
		rows.Close()
		if err != nil {
			return nil, err
		}
		res[tm.SourceTableIdentifier] = tableSchema
	}

	return res, nil
}

func (c *ClickHouseConnector) onCluster() string {
	if c.Config.Cluster != "" {
		return " ON CLUSTER " + peerdb_clickhouse.QuoteIdentifier(c.Config.Cluster)
	}
	return ""
}
