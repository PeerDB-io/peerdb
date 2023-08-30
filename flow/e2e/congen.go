package e2e

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	postgresHost     = "localhost"
	postgresUser     = "postgres"
	postgresPassword = "postgres"
	postgresDatabase = "postgres"
	PostgresPort     = 7132
)

func GetTestPostgresConf() *protos.PostgresConfig {
	return &protos.PostgresConfig{
		Host:     postgresHost,
		Port:     uint32(PostgresPort),
		User:     postgresUser,
		Password: postgresPassword,
		Database: postgresDatabase,
	}
}

func cleanPostgres(pool *pgxpool.Pool, suffix string) error {
	// drop the e2e_test schema with the given suffix if it exists
	_, err := pool.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS e2e_test_%s CASCADE", suffix))
	if err != nil {
		return fmt.Errorf("failed to drop e2e_test schema: %w", err)
	}

	// drop all open slots with the given suffix
	_, err = pool.Exec(
		context.Background(),
		"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE $1",
		fmt.Sprintf("%%_%s", suffix),
	)
	if err != nil {
		return fmt.Errorf("failed to drop replication slots: %w", err)
	}

	// list all publications from pg_publication table
	rows, err := pool.Query(context.Background(),
		"SELECT pubname FROM pg_publication WHERE pubname LIKE $1",
		fmt.Sprintf("%%_%s", suffix),
	)
	if err != nil {
		return fmt.Errorf("failed to list publications: %w", err)
	}

	// drop all publications with the given suffix
	for rows.Next() {
		var pubName string
		err = rows.Scan(&pubName)
		if err != nil {
			return fmt.Errorf("failed to scan publication name: %w", err)
		}

		_, err = pool.Exec(context.Background(), fmt.Sprintf("DROP PUBLICATION %s", pubName))
		if err != nil {
			return fmt.Errorf("failed to drop publication %s: %w", pubName, err)
		}
	}

	return nil
}

// setupPostgres sets up the postgres connection pool.
func SetupPostgres(suffix string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(context.Background(), utils.GetPGConnectionString(GetTestPostgresConf()))
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres connection pool: %w", err)
	}

	err = cleanPostgres(pool, suffix)
	if err != nil {
		return nil, err
	}

	// create an e2e_test schema
	_, err = pool.Exec(context.Background(), fmt.Sprintf("CREATE SCHEMA e2e_test_%s", suffix))
	if err != nil {
		return nil, fmt.Errorf("failed to create e2e_test schema: %w", err)
	}

	return pool, nil
}

func TearDownPostgres(pool *pgxpool.Pool, suffix string) error {
	// drop the e2e_test schema
	if pool != nil {
		err := cleanPostgres(pool, suffix)
		if err != nil {
			return err
		}
		pool.Close()
	}
	return nil
}

// GeneratePostgresPeer generates a postgres peer config for testing.
func GeneratePostgresPeer(postgresPort int) *protos.Peer {
	ret := &protos.Peer{}
	ret.Name = "test_postgres_peer"
	ret.Type = protos.DBType_POSTGRES

	ret.Config = &protos.Peer_PostgresConfig{
		PostgresConfig: &protos.PostgresConfig{
			Host:     "localhost",
			Port:     uint32(postgresPort),
			User:     "postgres",
			Password: "postgres",
			Database: "postgres",
		},
	}

	return ret
}

type FlowConnectionGenerationConfig struct {
	FlowJobName      string
	TableNameMapping map[string]string
	PostgresPort     int
	Destination      *protos.Peer
	CDCSyncMode      protos.QRepSyncMode
	CdcStagingPath   string
}

// GenerateSnowflakePeer generates a snowflake peer config for testing.
func GenerateSnowflakePeer(snowflakeConfig *protos.SnowflakeConfig) (*protos.Peer, error) {
	ret := &protos.Peer{}
	ret.Name = "test_snowflake_peer"
	ret.Type = protos.DBType_SNOWFLAKE

	ret.Config = &protos.Peer_SnowflakeConfig{
		SnowflakeConfig: snowflakeConfig,
	}

	return ret, nil
}

func (c *FlowConnectionGenerationConfig) GenerateFlowConnectionConfigs() (*protos.FlowConnectionConfigs, error) {
	ret := &protos.FlowConnectionConfigs{}
	ret.FlowJobName = c.FlowJobName
	ret.TableNameMapping = c.TableNameMapping
	ret.Source = GeneratePostgresPeer(c.PostgresPort)
	ret.Destination = c.Destination
	ret.CdcSyncMode = c.CDCSyncMode
	ret.CdcStagingPath = c.CdcStagingPath
	return ret, nil
}

type QRepFlowConnectionGenerationConfig struct {
	FlowJobName                string
	WatermarkTable             string
	DestinationTableIdentifier string
	PostgresPort               int
	Destination                *protos.Peer
	StagingPath                string
}

// GenerateQRepConfig generates a qrep config for testing.
func (c *QRepFlowConnectionGenerationConfig) GenerateQRepConfig(
	query string, watermark string, syncMode protos.QRepSyncMode) (*protos.QRepConfig, error) {
	ret := &protos.QRepConfig{}
	ret.FlowJobName = c.FlowJobName
	ret.WatermarkTable = c.WatermarkTable
	ret.DestinationTableIdentifier = c.DestinationTableIdentifier

	postgresPeer := GeneratePostgresPeer(c.PostgresPort)
	ret.SourcePeer = postgresPeer

	ret.DestinationPeer = c.Destination

	ret.Query = query
	ret.WatermarkColumn = watermark

	ret.SyncMode = syncMode
	ret.StagingPath = c.StagingPath

	return ret, nil
}
