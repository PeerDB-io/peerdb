package connsnowflake

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/snowflakedb/gosnowflake"

	peersql "github.com/PeerDB-io/peer-flow/connectors/sql"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	util "github.com/PeerDB-io/peer-flow/utils"
)

type SnowflakeClient struct {
	peersql.GenericSQLQueryExecutor
	// ctx is the context.
	ctx context.Context
	// config is the Snowflake config.
	Config *protos.SnowflakeConfig
}

func NewSnowflakeClient(ctx context.Context, config *protos.SnowflakeConfig) (*SnowflakeClient, error) {
	privateKey, err := util.DecodePKCS8PrivateKey([]byte(config.PrivateKey))
	if err != nil {
		return nil, fmt.Errorf("failed to read private key: %w", err)
	}

	snowflakeConfig := gosnowflake.Config{
		Account:          config.AccountId,
		User:             config.Username,
		Authenticator:    gosnowflake.AuthTypeJwt,
		PrivateKey:       privateKey,
		Database:         config.Database,
		Warehouse:        config.Warehouse,
		Role:             config.Role,
		RequestTimeout:   time.Duration(config.QueryTimeout) * time.Second,
		DisableTelemetry: true,
	}

	snowflakeConfigDSN, err := gosnowflake.DSN(&snowflakeConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get DSN from Snowflake config: %w", err)
	}

	database, err := sqlx.Open("snowflake", snowflakeConfigDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to Snowflake peer: %w", err)
	}

	err = database.PingContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to Snowflake peer: %w", err)
	}

	genericExecutor := *peersql.NewGenericSQLQueryExecutor(
		ctx, database, snowflakeTypeToQValueKindMap, qValueKindToSnowflakeTypeMap)

	return &SnowflakeClient{
		GenericSQLQueryExecutor: genericExecutor,
		ctx:                     ctx,
		Config:                  config,
	}, nil
}
