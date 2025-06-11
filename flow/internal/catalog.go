package internal

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

var (
	poolMutex = &sync.Mutex{}
	pool      *pgxpool.Pool
)

func GetCatalogConnectionPoolFromEnv(ctx context.Context) (shared.CatalogPool, error) {
	poolMutex.Lock()
	defer poolMutex.Unlock()
	if pool == nil {
		var err error
		catalogConnectionString := GetCatalogConnectionStringFromEnv(ctx)
		pool, err = pgxpool.New(ctx, catalogConnectionString)
		if err != nil {
			return shared.CatalogPool{Pool: pool},
				exceptions.NewCatalogError(fmt.Errorf("unable to establish connection with catalog: %w", err))
		}
	}

	if err := pool.Ping(ctx); err != nil {
		return shared.CatalogPool{Pool: pool},
			exceptions.NewCatalogError(fmt.Errorf("unable to establish connection with catalog: %w", err))
	}

	return shared.CatalogPool{Pool: pool}, nil
}

func GetCatalogConnectionStringFromEnv(ctx context.Context) string {
	return GetPGConnectionString(GetCatalogPostgresConfigFromEnv(ctx), "")
}

func GetCatalogPostgresConfigFromEnv(ctx context.Context) *protos.PostgresConfig {
	return &protos.PostgresConfig{
		Host:       PeerDBCatalogHost(),
		Port:       uint32(PeerDBCatalogPort()),
		User:       PeerDBCatalogUser(),
		Password:   PeerDBCatalogPassword(ctx),
		Database:   PeerDBCatalogDatabase(),
		RequireTls: PeerDBCatalogRequireTls(),
	}
}
