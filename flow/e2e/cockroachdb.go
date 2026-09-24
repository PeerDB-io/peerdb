package e2e

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	conncockroachdb "github.com/PeerDB-io/peerdb/flow/connectors/cockroachdb"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type CockroachDBSource struct {
	conn   *conncockroachdb.CockroachDBConnector
	config *protos.CockroachDBConfig

	// separate connection for seeding test data
	adminConn *pgx.Conn
}

func (s *CockroachDBSource) GeneratePeer(t *testing.T) *protos.Peer {
	t.Helper()
	peer := &protos.Peer{
		Name: "cockroachdb",
		Type: protos.DBType_COCKROACHDB,
		Config: &protos.Peer_CockroachdbConfig{
			CockroachdbConfig: s.config,
		},
	}
	CreatePeer(t, peer)
	return peer
}

func (s *CockroachDBSource) Teardown(t *testing.T, ctx context.Context, suffix string) {
	t.Helper()
	if s.adminConn != nil {
		_, _ = s.adminConn.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS e2e_test_%s CASCADE", suffix))
		_ = s.adminConn.Close(ctx)
	}
	_ = s.conn.Close()
}

func (s *CockroachDBSource) Connector() connectors.Connector {
	return s.conn
}

func (s *CockroachDBSource) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := s.adminConn.Exec(ctx, sql, args...)
	return err
}

func (s *CockroachDBSource) GetRows(ctx context.Context, suffix, table, cols string) (*model.QRecordBatch, error) {
	tableName := fmt.Sprintf("e2e_test_%s.%s", suffix, table)
	config := &protos.QRepConfig{
		FlowJobName:    "testflow",
		WatermarkTable: tableName,
		Query:          fmt.Sprintf("SELECT %s FROM %s ORDER BY id", cols, tableName),
		Version:        shared.InternalVersion_Latest,
	}
	partition := &protos.QRepPartition{
		PartitionId:        "testpart",
		FullTablePartition: true,
	}

	stream := model.NewQRecordStream(1024)
	go func() {
		_, _, err := s.conn.PullQRepRecords(ctx, shared.CatalogPool{}, nil, config, protos.DBType_CLICKHOUSE, partition, stream)
		stream.Close(err)
	}()

	schema, err := stream.Schema()
	if err != nil {
		return nil, err
	}
	batch := &model.QRecordBatch{
		Schema:  schema,
		Records: nil,
	}
	for record := range stream.Records {
		batch.Records = append(batch.Records, record)
	}
	if err := stream.Err(); err != nil {
		return nil, err
	}
	return batch, nil
}

func SetupCockroachDB(t *testing.T, suffix string) (*CockroachDBSource, error) {
	t.Helper()
	return setupCockroachDBWithConfig(t, suffix, internal.GetCockroachDBConfigFromEnv())
}

func setupCockroachDBWithConfig(
	t *testing.T, suffix string, config *protos.CockroachDBConfig,
) (*CockroachDBSource, error) {
	t.Helper()
	connector, err := conncockroachdb.NewCockroachDBConnector(t.Context(), nil, config)
	if err != nil {
		return nil, fmt.Errorf("failed to setup cockroachdb connector: %w", err)
	}

	adminConn, err := pgx.Connect(t.Context(), conncockroachdb.GetCRDBConnectionString(config, ""))
	if err != nil {
		connector.Close()
		return nil, fmt.Errorf("failed to setup cockroachdb admin connection: %w", err)
	}

	if _, err := adminConn.Exec(t.Context(),
		fmt.Sprintf("DROP SCHEMA IF EXISTS e2e_test_%s CASCADE", suffix),
	); err != nil {
		connector.Close()
		_ = adminConn.Close(t.Context())
		return nil, fmt.Errorf("failed to drop e2e_test schema: %w", err)
	}
	if _, err := adminConn.Exec(t.Context(), "CREATE SCHEMA e2e_test_"+suffix); err != nil {
		connector.Close()
		_ = adminConn.Close(t.Context())
		return nil, fmt.Errorf("failed to create e2e_test schema: %w", err)
	}

	return &CockroachDBSource{conn: connector, config: config, adminConn: adminConn}, nil
}

// CockroachDBTestContainerConfig starts an isolated source for tests that change cluster settings.
// CI and Tilt pass COCKROACHDB_IMAGE so this test covers the same version as the shared source.
func CockroachDBTestContainerConfig(t *testing.T) *protos.CockroachDBConfig {
	t.Helper()
	image := os.Getenv("COCKROACHDB_IMAGE")
	ctr, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			Cmd:          []string{"start-single-node", "--insecure"},
			ExposedPorts: []string{"26257/tcp"},
			WaitingFor: wait.ForExec([]string{"./cockroach", "sql", "--insecure", "-e", "SELECT 1"}).
				WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	})
	testcontainers.CleanupContainer(t, ctr, testcontainers.StopTimeout(30*time.Second))
	require.NoError(t, err)
	host, err := ctr.Host(t.Context())
	require.NoError(t, err)
	mappedPort, err := ctr.MappedPort(t.Context(), "26257/tcp")
	require.NoError(t, err)
	port, err := strconv.ParseUint(mappedPort.Port(), 10, 32)
	require.NoError(t, err)

	config := internal.GetCockroachDBConfigFromEnv()
	config.Host = host
	config.Port = uint32(port)
	conn, err := pgx.Connect(t.Context(), conncockroachdb.GetCRDBConnectionString(config, ""))
	require.NoError(t, err)
	// Match local_provision_scripts/cockroachdb.sh: single-node CockroachDB
	// defaults to a four-hour GC window, below the connector's 24-hour floor.
	_, err = conn.Exec(t.Context(), "ALTER RANGE default CONFIGURE ZONE USING gc.ttlseconds = 90000")
	require.NoError(t, err)
	_, err = conn.Exec(t.Context(), "SET CLUSTER SETTING kv.rangefeed.enabled = true")
	require.NoError(t, err)
	require.NoError(t, conn.Close(t.Context()))
	return config
}
