package connclickhouse

import (
	"context"
	"fmt"

	peersql "github.com/PeerDB-io/peer-flow/connectors/sql"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jmoiron/sqlx"
)

type ClickhouseClient struct {
	peersql.GenericSQLQueryExecutor
	// ctx is the context.
	ctx context.Context
	// config is the Snowflake config.
	Config *protos.ClickhouseConfig
}

func NewClickhouseClient(ctx context.Context, config *protos.ClickhouseConfig) (*ClickhouseClient, error) {
	// privateKey, err := shared.DecodePKCS8PrivateKey([]byte(config.PrivateKey), config.Password)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to read private key: %w", err)
	// }

	// snowflakeConfig := gosnowflake.Config{
	// 	Account:          config.AccountId,
	// 	User:             config.Username,
	// 	Authenticator:    gosnowflake.AuthTypeJwt,
	// 	PrivateKey:       privateKey,
	// 	Database:         config.Database,
	// 	Warehouse:        config.Warehouse,
	// 	Role:             config.Role,
	// 	RequestTimeout:   time.Duration(config.QueryTimeout) * time.Second,
	// 	DisableTelemetry: true,
	// }

	// snowflakeConfigDSN, err := gosnowflake.DSN(&snowflakeConfig)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to get DSN from Snowflake config: %w", err)
	// }

	// database, err := sqlx.Open("snowflake", snowflakeConfigDSN)
	databaseSql, err := connect(ctx, config)
	database := sqlx.NewDb(databaseSql, "clickhouse") // Use the appropriate driver name

	if err != nil {
		return nil, fmt.Errorf("failed to open connection to Snowflake peer: %w", err)
	}

	// err = database.PingContext(ctx)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to open connection to Snowflake peer: %w", err)
	// }

	genericExecutor := *peersql.NewGenericSQLQueryExecutor(
		ctx, database, clickhouseTypeToQValueKindMap, qvalue.QValueKindToSnowflakeTypeMap)

	return &ClickhouseClient{
		GenericSQLQueryExecutor: genericExecutor,
		ctx:                     ctx,
		Config:                  config,
	}, nil
}

// func connect(ctx, config *protos.ClickhouseConfig) (*sql.DB, error) {
// 	host := config.host
// 	port := config.port
// 	conn, err := clickhouse.Open(&clickhouse.Options{
// 		Addr: []string{host + ":" + port},
// 		Auth: clickhouse.Auth{
// 			Database: config.database,
// 			Username: config.user,
// 			Password: config.password,
// 		},
// 	})

// 	if err != nil {
// 		return nil, err
// 	}

// 	if err := conn.Ping(ctx); err != nil {
// 		if exception, ok := err.(*clickhouse.Exception); ok {
// 			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
// 		}
// 		return nil, err
// 	}
// 	return conn, nil
// }

func (c *ClickhouseConnector) getTableCounts(tables []string) (int64, error) {
	var totalRecords int64
	for _, table := range tables {
		_, err := parseTableName(table)
		if err != nil {
			return 0, fmt.Errorf("failed to parse table name %s: %w", table, err)
		}
		//nolint:gosec
		row := c.database.QueryRowContext(c.ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", table))
		var count pgtype.Int8
		err = row.Scan(&count)
		if err != nil {
			return 0, fmt.Errorf("failed to get count for table %s: %w", table, err)
		}
		totalRecords += count.Int64
	}
	return totalRecords, nil
}
