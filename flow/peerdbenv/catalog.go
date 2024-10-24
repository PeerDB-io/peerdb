package peerdbenv

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
)

var (
	poolMutex = &sync.Mutex{}
	pool      *pgxpool.Pool
)

func GetCatalogConnectionPoolFromEnv(ctx context.Context) (*pgxpool.Pool, error) {
	var err error

	poolMutex.Lock()
	defer poolMutex.Unlock()
	if pool == nil {
		catalogConnectionString := GetCatalogConnectionStringFromEnv(ctx)
		pool, err = pgxpool.New(ctx, catalogConnectionString)
		if err != nil {
			return nil, fmt.Errorf("unable to establish connection with catalog: %w", err)
		}
	}

	err = pool.Ping(ctx)
	if err != nil {
		return pool, fmt.Errorf("unable to establish connection with catalog: %w", err)
	}

	return pool, nil
}

func GetCatalogConnectionStringFromEnv(ctx context.Context) string {
	return shared.GetPGConnectionString(GetCatalogPostgresConfigFromEnv(ctx), "")
}

func GetCatalogPostgresConfigFromEnv(ctx context.Context) *protos.PostgresConfig {
	return &protos.PostgresConfig{
		Host:     PeerDBCatalogHost(),
		Port:     uint32(PeerDBCatalogPort()),
		User:     PeerDBCatalogUser(),
		Password: PeerDBCatalogPassword(ctx),
		Database: PeerDBCatalogDatabase(),
	}
}
