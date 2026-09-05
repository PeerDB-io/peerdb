package internal

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

var (
	poolMutex = &sync.Mutex{}
	pool      atomic.Pointer[pgxpool.Pool]
)

func GetCatalogConnectionPoolFromEnv(ctx context.Context) (shared.CatalogPool, error) {
	if pool.Load() == nil {
		poolMutex.Lock()
		defer poolMutex.Unlock()
		if pool.Load() == nil {
			var err error
			catalogConnectionString := GetCatalogConnectionStringFromEnv(ctx)
			config, err := pgxpool.ParseConfig(catalogConnectionString)
			if err != nil {
				return shared.CatalogPool{},
					exceptions.NewCatalogError(fmt.Errorf("unable to parse catalog connection string: %w", err))
			}
			config.MaxConns = 3
			config.MaxConnIdleTime = 90 * time.Second
			localPool, err := pgxpool.NewWithConfig(ctx, config)
			if err != nil {
				return shared.CatalogPool{},
					exceptions.NewCatalogError(fmt.Errorf("unable to initialize catalog connection pool: %w", err))
			}

			if err := localPool.Ping(ctx); err != nil {
				localPool.Close()
				return shared.CatalogPool{},
					exceptions.NewCatalogError(fmt.Errorf("unable to establish connection with catalog: %w", err))
			}
			pool.Store(localPool)
		}
	}

	return shared.CatalogPool{Pool: pool.Load()}, nil
}

func GetCatalogConnectionStringFromEnv(ctx context.Context) string {
	return GetPGConnectionString(GetCatalogPostgresConfigFromEnv(ctx), "catalog_test_access")
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
