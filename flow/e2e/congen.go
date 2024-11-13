package e2e

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

func cleanPostgres(conn *pgx.Conn, suffix string) error {
	// drop the e2e_test schema with the given suffix if it exists
	if _, err := conn.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS e2e_test_%s CASCADE", suffix)); err != nil {
		return fmt.Errorf("failed to drop e2e_test schema: %w", err)
	}

	// drop all open slots with the given suffix
	if _, err := conn.Exec(
		context.Background(),
		"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE $1",
		"%_"+suffix,
	); err != nil {
		return fmt.Errorf("failed to drop replication slots: %w", err)
	}

	// list all publications from pg_publication table
	rows, err := conn.Query(context.Background(),
		"SELECT pubname FROM pg_publication WHERE pubname LIKE $1",
		"%_"+suffix,
	)
	if err != nil {
		return fmt.Errorf("failed to list publications: %w", err)
	}
	publications, err := pgx.CollectRows[string](rows, pgx.RowTo)
	if err != nil {
		return fmt.Errorf("failed to read publications: %w", err)
	}

	for _, pubName := range publications {
		if _, err := conn.Exec(context.Background(), "DROP PUBLICATION "+pubName); err != nil {
			return fmt.Errorf("failed to drop publication %s: %w", pubName, err)
		}
	}

	return nil
}

func setupPostgresSchema(t *testing.T, conn *pgx.Conn, suffix string) error {
	t.Helper()

	setupTx, err := conn.Begin(context.Background())
	if err != nil {
		return errors.New("failed to start setup transaction")
	}

	// create an e2e_test schema
	if _, err := setupTx.Exec(context.Background(), "SELECT pg_advisory_xact_lock(hashtext('Megaton Mile'))"); err != nil {
		return fmt.Errorf("failed to get lock: %w", err)
	}
	defer func() {
		deferErr := setupTx.Rollback(context.Background())
		if deferErr != pgx.ErrTxClosed && deferErr != nil {
			t.Errorf("error rolling back setup transaction: %v", err)
		}
	}()

	// create an e2e_test schema
	if _, err := setupTx.Exec(context.Background(), "CREATE SCHEMA e2e_test_"+suffix); err != nil {
		return fmt.Errorf("failed to create e2e_test schema: %w", err)
	}

	if _, err := setupTx.Exec(context.Background(), `
		CREATE OR REPLACE FUNCTION random_string( int ) RETURNS TEXT as $$
			SELECT string_agg(substring('0123456789bcdfghjkmnpqrstvwxyz',
			round(random() * 30)::integer, 1), '') FROM generate_series(1, $1);
		$$ language sql;
		CREATE OR REPLACE FUNCTION random_bytea(bytea_length integer)
		RETURNS bytea AS $body$
			SELECT decode(string_agg(lpad(to_hex(width_bucket(random(), 0, 1, 256)-1),2,'0'), ''), 'hex')
			FROM generate_series(1, $1);
		$body$
		LANGUAGE 'sql'
		VOLATILE
		SET search_path = 'pg_catalog';
	`); err != nil {
		return fmt.Errorf("failed to create utility functions: %w", err)
	}

	return setupTx.Commit(context.Background())
}

// SetupPostgres sets up the postgres connection.
func SetupPostgres(t *testing.T, suffix string) (*connpostgres.PostgresConnector, error) {
	t.Helper()

	connector, err := connpostgres.NewPostgresConnector(context.Background(),
		nil, peerdbenv.GetCatalogPostgresConfigFromEnv(context.Background()))
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres connection: %w", err)
	}
	conn := connector.Conn()

	if err := cleanPostgres(conn, suffix); err != nil {
		connector.Close()
		return nil, err
	}

	if err := setupPostgresSchema(t, conn, suffix); err != nil {
		connector.Close()
		return nil, err
	}

	return connector, nil
}

func TearDownPostgres[T Suite](s T) {
	t := s.T()
	t.Helper()

	conn := s.Connector().Conn()
	if conn != nil {
		suffix := s.Suffix()
		t.Log("begin tearing down postgres schema", suffix)
		deadline := time.Now().Add(2 * time.Minute)
		for {
			err := cleanPostgres(conn, suffix)
			if err == nil {
				conn.Close(context.Background())
				return
			} else if time.Now().After(deadline) {
				require.Fail(t, "failed to teardown postgres schema", "%s: %v", suffix, err)
			}
			time.Sleep(time.Second)
		}
	}
}

// GeneratePostgresPeer generates a postgres peer config for testing.
func GeneratePostgresPeer(t *testing.T) *protos.Peer {
	t.Helper()
	peer := &protos.Peer{
		Name: "catalog",
		Type: protos.DBType_POSTGRES,
		Config: &protos.Peer_PostgresConfig{
			PostgresConfig: peerdbenv.GetCatalogPostgresConfigFromEnv(context.Background()),
		},
	}
	CreatePeer(t, peer)
	return peer
}

type FlowConnectionGenerationConfig struct {
	FlowJobName      string
	TableNameMapping map[string]string
	Destination      string
	TableMappings    []*protos.TableMapping
	SoftDelete       bool
}

func TableMappings(s GenericSuite, tables ...string) []*protos.TableMapping {
	if len(tables)&1 != 0 {
		panic("must receive even number of table names")
	}
	tm := make([]*protos.TableMapping, 0, len(tables)/2)
	for i := 0; i < len(tables); i += 2 {
		tm = append(tm, &protos.TableMapping{
			SourceTableIdentifier:      AttachSchema(s, tables[i]),
			DestinationTableIdentifier: s.DestinationTable(tables[i+1]),
		})
	}
	return tm
}

func CreatePeer(t *testing.T, peer *protos.Peer) {
	t.Helper()
	ctx := context.Background()
	pool, err := peerdbenv.GetCatalogConnectionPoolFromEnv(ctx)
	require.NoError(t, err)
	res, err := utils.CreatePeerNoValidate(ctx, pool, peer, false)
	require.NoError(t, err)
	if res.Status != protos.CreatePeerStatus_CREATED {
		require.Fail(t, res.Message)
	}
}

func (c *FlowConnectionGenerationConfig) GenerateFlowConnectionConfigs(t *testing.T) *protos.FlowConnectionConfigs {
	t.Helper()
	tblMappings := c.TableMappings
	if tblMappings == nil {
		for k, v := range c.TableNameMapping {
			tblMappings = append(tblMappings, &protos.TableMapping{
				SourceTableIdentifier:      k,
				DestinationTableIdentifier: v,
			})
		}
	}

	ret := &protos.FlowConnectionConfigs{
		FlowJobName:        c.FlowJobName,
		TableMappings:      tblMappings,
		SourceName:         GeneratePostgresPeer(t).Name,
		DestinationName:    c.Destination,
		SyncedAtColName:    "_PEERDB_SYNCED_AT",
		IdleTimeoutSeconds: 15,
	}
	if c.SoftDelete {
		ret.SoftDeleteColName = "_PEERDB_IS_DELETED"
	}
	return ret
}
