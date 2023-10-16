package utils

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/jackc/pgx/v5/pgxpool"
)

func GetCatalogConnectionPoolFromEnv() (*pgxpool.Pool, error) {
	catalogConnectionString, err := genCatalogConnectionString()
	if err != nil {
		return nil, fmt.Errorf("unable to generate catalog connection string: %w", err)
	}

	catalogConn, err := pgxpool.New(context.Background(), catalogConnectionString)
	if err != nil {
		return nil, fmt.Errorf("unable to establish connection with catalog: %w", err)
	}

	err = catalogConn.Ping(context.Background())
	if err != nil {
		return nil, fmt.Errorf("unable to establish connection with catalog: %w", err)
	}

	return catalogConn, nil
}

func genCatalogConnectionString() (string, error) {
	host, ok := os.LookupEnv("PEERDB_CATALOG_HOST")
	if !ok {
		return "", fmt.Errorf("PEERDB_CATALOG_HOST is not set")
	}
	portStr, ok := os.LookupEnv("PEERDB_CATALOG_PORT")
	if !ok {
		return "", fmt.Errorf("PEERDB_CATALOG_PORT is not set")
	}
	port, err := strconv.ParseUint(portStr, 10, 32)
	if err != nil {
		return "", fmt.Errorf("unable to parse PEERDB_CATALOG_PORT as unsigned integer")
	}
	user, ok := os.LookupEnv("PEERDB_CATALOG_USER")
	if !ok {
		return "", fmt.Errorf("PEERDB_CATALOG_USER is not set")
	}
	password, ok := os.LookupEnv("PEERDB_CATALOG_PASSWORD")
	if !ok {
		return "", fmt.Errorf("PEERDB_CATALOG_PASSWORD is not set")
	}
	database, ok := os.LookupEnv("PEERDB_CATALOG_DATABASE")
	if !ok {
		return "", fmt.Errorf("PEERDB_CATALOG_DATABASE is not set")
	}

	return utils.GetPGConnectionString(&protos.PostgresConfig{
		Host:     host,
		Port:     uint32(port),
		User:     user,
		Password: password,
		Database: database,
	}), nil
}
