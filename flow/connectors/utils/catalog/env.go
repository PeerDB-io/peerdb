package utils

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

var (
	poolMutex = &sync.Mutex{}
	pool      *pgxpool.Pool
)

func GetCatalogConnectionPoolFromEnv() (*pgxpool.Pool, error) {
	var err error

	poolMutex.Lock()
	defer poolMutex.Unlock()
	if pool == nil {
		catalogConnectionString := genCatalogConnectionString()
		pool, err = pgxpool.New(context.Background(), catalogConnectionString)
		if err != nil {
			return nil, fmt.Errorf("unable to establish connection with catalog: %w", err)
		}
	}

	err = pool.Ping(context.Background())
	if err != nil {
		return pool, fmt.Errorf("unable to establish connection with catalog: %w", err)
	}

	return pool, nil
}

func genCatalogConnectionString() string {
	return utils.GetPGConnectionString(&protos.PostgresConfig{
		Host:     peerdbenv.PeerDBCatalogHost(),
		Port:     peerdbenv.PeerDBCatalogPort(),
		User:     peerdbenv.PeerDBCatalogUser(),
		Password: peerdbenv.PeerDBCatalogPassword(),
		Database: peerdbenv.PeerDBCatalogDatabase(),
	})
}
