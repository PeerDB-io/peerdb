package connclickhouse

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"

	peersql "github.com/PeerDB-io/peer-flow/connectors/sql"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

type ClickhouseClient struct {
	peersql.GenericSQLQueryExecutor
	// ctx is the context.
	ctx context.Context
	// config is the Snowflake config.
	Config *protos.ClickhouseConfig
}

func NewClickhouseClient(ctx context.Context, config *protos.ClickhouseConfig) (*ClickhouseClient, error) {
	databaseSql, err := connect(ctx, config)
	database := sqlx.NewDb(databaseSql, "clickhouse") // Use the appropriate driver name

	if err != nil {
		return nil, fmt.Errorf("failed to open connection to Snowflake peer: %w", err)
	}

	genericExecutor := *peersql.NewGenericSQLQueryExecutor(
		ctx, database, clickhouseTypeToQValueKindMap, qvalue.QValueKindToSnowflakeTypeMap)

	return &ClickhouseClient{
		GenericSQLQueryExecutor: genericExecutor,
		ctx:                     ctx,
		Config:                  config,
	}, nil
}
