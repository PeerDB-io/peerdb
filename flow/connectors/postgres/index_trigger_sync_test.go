package connpostgres

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type IndexTriggerSyncTestSuite struct {
	t            *testing.T
	sourceConn   *PostgresConnector
	destConn     *PostgresConnector
	sourceSchema string
	destSchema   string
}

func SetupIndexTriggerSyncSuite(t *testing.T) IndexTriggerSyncTestSuite {
	t.Helper()
	sourceConn, err := NewPostgresConnector(t.Context(), nil, internal.GetCatalogPostgresConfigFromEnv(t.Context()))
	require.NoError(t, err)
	destConn, err := NewPostgresConnector(t.Context(), nil, internal.GetCatalogPostgresConfigFromEnv(t.Context()))
	require.NoError(t, err)
	sourceSchema := "src_idx_" + strings.ToLower(shared.RandomString(8))
	destSchema := "dst_idx_" + strings.ToLower(shared.RandomString(8))

	setupTx, err := sourceConn.conn.Begin(t.Context())
	require.NoError(t, err)
	defer func() {
		err := setupTx.Rollback(t.Context())
		if err != pgx.ErrTxClosed {
			require.NoError(t, err)
		}
	}()

	_, err = setupTx.Exec(t.Context(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", sourceSchema))
	require.NoError(t, err)
	_, err = setupTx.Exec(t.Context(), "CREATE SCHEMA "+sourceSchema)
	require.NoError(t, err)
	require.NoError(t, setupTx.Commit(t.Context()))

	setupTx2, err := destConn.conn.Begin(t.Context())
	require.NoError(t, err)
	defer func() {
		err := setupTx2.Rollback(t.Context())
		if err != pgx.ErrTxClosed {
			require.NoError(t, err)
		}
	}()

	_, err = setupTx2.Exec(t.Context(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", destSchema))
	require.NoError(t, err)
	_, err = setupTx2.Exec(t.Context(), "CREATE SCHEMA "+destSchema)
	require.NoError(t, err)
	require.NoError(t, setupTx2.Commit(t.Context()))

	return IndexTriggerSyncTestSuite{
		t:            t,
		sourceConn:   sourceConn,
		destConn:     destConn,
		sourceSchema: sourceSchema,
		destSchema:   destSchema,
	}
}

func (s IndexTriggerSyncTestSuite) Teardown(ctx context.Context) {
	_, _ = s.sourceConn.conn.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", s.sourceSchema))
	_, _ = s.destConn.conn.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", s.destSchema))
	require.NoError(s.t, s.sourceConn.Close())
	require.NoError(s.t, s.destConn.Close())
}

func (s IndexTriggerSyncTestSuite) TestSyncIndexes() {
	ctx := s.t.Context()
	sourceTable := s.sourceSchema + ".test_table"
	destTable := s.destSchema + ".test_table"
	_, err := s.sourceConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			name TEXT,
			email TEXT,
			created_at TIMESTAMPTZ DEFAULT NOW()
		)`, sourceTable))
	require.NoError(s.t, err)
	_, err = s.sourceConn.conn.Exec(ctx, fmt.Sprintf("CREATE INDEX idx_name ON %s(name)", sourceTable))
	require.NoError(s.t, err)

	_, err = s.sourceConn.conn.Exec(ctx, fmt.Sprintf("CREATE UNIQUE INDEX idx_email ON %s(email)", sourceTable))
	require.NoError(s.t, err)

	_, err = s.sourceConn.conn.Exec(ctx, fmt.Sprintf("CREATE INDEX idx_created_at ON %s(created_at DESC)", sourceTable))
	require.NoError(s.t, err)
	_, err = s.destConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			name TEXT,
			email TEXT,
			created_at TIMESTAMPTZ DEFAULT NOW()
		)`, destTable))
	require.NoError(s.t, err)

	tableMappings := []*protos.TableMapping{
		{
			SourceTableIdentifier:      sourceTable,
			DestinationTableIdentifier: destTable,
		},
	}

	err = s.destConn.SyncIndexesAndTriggers(ctx, tableMappings, s.sourceConn)
	require.NoError(s.t, err)
	destTableParsed, err := utils.ParseSchemaTable(destTable)
	require.NoError(s.t, err)
	destIndexes, err := s.destConn.getIndexesForTable(ctx, destTableParsed)
	require.NoError(s.t, err)
	indexNames := make(map[string]bool)
	for _, idx := range destIndexes {
		indexNames[idx.IndexName] = true
	}
	require.True(s.t, indexNames["test_table_pkey"], "Primary key should exist")
	require.True(s.t, indexNames["idx_name"], "idx_name should be synced")
	require.True(s.t, indexNames["idx_email"], "idx_email should be synced")
	require.True(s.t, indexNames["idx_created_at"], "idx_created_at should be synced")
}

func (s IndexTriggerSyncTestSuite) TestSyncTriggers() {
	ctx := s.t.Context()
	sourceTable := s.sourceSchema + ".test_trigger_table"
	destTable := s.destSchema + ".test_trigger_table"
	_, err := s.sourceConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION %s.update_timestamp()
		RETURNS TRIGGER AS $$
		BEGIN
			NEW.updated_at = NOW();
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql`, s.sourceSchema))
	require.NoError(s.t, err)
	_, err = s.destConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION %s.update_timestamp()
		RETURNS TRIGGER AS $$
		BEGIN
			NEW.updated_at = NOW();
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql`, s.destSchema))
	require.NoError(s.t, err)
	_, err = s.sourceConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			name TEXT,
			updated_at TIMESTAMPTZ DEFAULT NOW()
		)`, sourceTable))
	require.NoError(s.t, err)

	_, err = s.sourceConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TRIGGER update_updated_at
		BEFORE UPDATE ON %s
		FOR EACH ROW
		EXECUTE FUNCTION %s.update_timestamp()`, sourceTable, s.sourceSchema))
	require.NoError(s.t, err)
	_, err = s.destConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			name TEXT,
			updated_at TIMESTAMPTZ DEFAULT NOW()
		)`, destTable))
	require.NoError(s.t, err)
	tableMappings := []*protos.TableMapping{
		{
			SourceTableIdentifier:      sourceTable,
			DestinationTableIdentifier: destTable,
		},
	}

	err = s.destConn.SyncIndexesAndTriggers(ctx, tableMappings, s.sourceConn)
	require.NoError(s.t, err)
	destTableParsed, err := utils.ParseSchemaTable(destTable)
	require.NoError(s.t, err)
	destTriggers, err := s.destConn.getTriggersForTable(ctx, destTableParsed)
	require.NoError(s.t, err)
	require.Len(s.t, destTriggers, 1, "Should have 1 trigger")
	require.Equal(s.t, "update_updated_at", destTriggers[0].TriggerName)
}

func (s IndexTriggerSyncTestSuite) TestSyncIndexesAndTriggersTogether() {
	ctx := s.t.Context()
	sourceTable := s.sourceSchema + ".test_both_table"
	destTable := s.destSchema + ".test_both_table"

	// Create function for trigger
	_, err := s.sourceConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION %s.log_changes()
		RETURNS TRIGGER AS $$
		BEGIN
			-- Simple trigger function
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql`, s.sourceSchema))
	require.NoError(s.t, err)

	_, err = s.destConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION %s.log_changes()
		RETURNS TRIGGER AS $$
		BEGIN
			-- Simple trigger function
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql`, s.destSchema))
	require.NoError(s.t, err)
	_, err = s.sourceConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			data TEXT
		)`, sourceTable))
	require.NoError(s.t, err)

	_, err = s.sourceConn.conn.Exec(ctx, fmt.Sprintf("CREATE INDEX idx_data ON %s(data)", sourceTable))
	require.NoError(s.t, err)

	_, err = s.sourceConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TRIGGER log_trigger
		AFTER INSERT ON %s
		FOR EACH ROW
		EXECUTE FUNCTION %s.log_changes()`, sourceTable, s.sourceSchema))
	require.NoError(s.t, err)
	_, err = s.destConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			data TEXT
		)`, destTable))
	require.NoError(s.t, err)
	tableMappings := []*protos.TableMapping{
		{
			SourceTableIdentifier:      sourceTable,
			DestinationTableIdentifier: destTable,
		},
	}

	err = s.destConn.SyncIndexesAndTriggers(ctx, tableMappings, s.sourceConn)
	require.NoError(s.t, err)
	destTableParsed, err := utils.ParseSchemaTable(destTable)
	require.NoError(s.t, err)
	destIndexes, err := s.destConn.getIndexesForTable(ctx, destTableParsed)
	require.NoError(s.t, err)

	indexNames := make(map[string]bool)
	for _, idx := range destIndexes {
		indexNames[idx.IndexName] = true
	}
	require.True(s.t, indexNames["idx_data"], "idx_data should be synced")
	destTriggers, err := s.destConn.getTriggersForTable(ctx, destTableParsed)
	require.NoError(s.t, err)

	require.Len(s.t, destTriggers, 1, "Should have 1 trigger")
	require.Equal(s.t, "log_trigger", destTriggers[0].TriggerName)
}

func (s IndexTriggerSyncTestSuite) TestSyncCheckConstraints() {
	ctx := s.t.Context()
	sourceTable := s.sourceSchema + ".test_check_table"
	destTable := s.destSchema + ".test_check_table"
	_, err := s.sourceConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			name TEXT NOT NULL,
			age INT,
			email TEXT
		)`, sourceTable))
	require.NoError(s.t, err)
	_, err = s.sourceConn.conn.Exec(ctx, fmt.Sprintf(`
		ALTER TABLE %s ADD CONSTRAINT check_name_length 
		CHECK (char_length(name) >= 3)`, sourceTable))
	require.NoError(s.t, err)

	_, err = s.sourceConn.conn.Exec(ctx, fmt.Sprintf(`
		ALTER TABLE %s ADD CONSTRAINT check_age_positive 
		CHECK (age > 0 AND age < 150)`, sourceTable))
	require.NoError(s.t, err)

	_, err = s.sourceConn.conn.Exec(ctx, fmt.Sprintf(`
		ALTER TABLE %s ADD CONSTRAINT check_email_format 
		CHECK (email IS NULL OR email LIKE '%%@%%')`, sourceTable))
	require.NoError(s.t, err)
	_, err = s.destConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			name TEXT NOT NULL,
			age INT,
			email TEXT
		)`, destTable))
	require.NoError(s.t, err)

	tableMappings := []*protos.TableMapping{
		{
			SourceTableIdentifier:      sourceTable,
			DestinationTableIdentifier: destTable,
		},
	}

	err = s.destConn.SyncIndexesAndTriggers(ctx, tableMappings, s.sourceConn)
	require.NoError(s.t, err)
	destTableParsed, err := utils.ParseSchemaTable(destTable)
	require.NoError(s.t, err)
	destConstraints, err := s.destConn.getConstraintsForTable(ctx, destTableParsed)
	require.NoError(s.t, err)
	constraintNames := make(map[string]bool)
	for _, constraint := range destConstraints {
		if constraint.ConstraintType == "c" {
			constraintNames[constraint.ConstraintName] = true
		}
	}

	require.True(s.t, constraintNames["check_name_length"], "check_name_length should be synced")
	require.True(s.t, constraintNames["check_age_positive"], "check_age_positive should be synced")
	require.True(s.t, constraintNames["check_email_format"], "check_email_format should be synced")
}

func (s IndexTriggerSyncTestSuite) TestSyncForeignKeyConstraints() {
	ctx := s.t.Context()
	parentSourceTable := s.sourceSchema + ".parent_table"
	parentDestTable := s.destSchema + ".parent_table"

	_, err := s.sourceConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			name TEXT
		)`, parentSourceTable))
	require.NoError(s.t, err)

	_, err = s.destConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			name TEXT
		)`, parentDestTable))
	require.NoError(s.t, err)
	childSourceTable := s.sourceSchema + ".child_table"
	childDestTable := s.destSchema + ".child_table"

	_, err = s.sourceConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			parent_id INT,
			name TEXT
		)`, childSourceTable))
	require.NoError(s.t, err)
	_, err = s.sourceConn.conn.Exec(ctx, fmt.Sprintf(`
		ALTER TABLE %s ADD CONSTRAINT fk_parent 
		FOREIGN KEY (parent_id) REFERENCES %s(id)`, childSourceTable, parentSourceTable))
	require.NoError(s.t, err)
	_, err = s.destConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			parent_id INT,
			name TEXT
		)`, childDestTable))
	require.NoError(s.t, err)
	tableMappings := []*protos.TableMapping{
		{
			SourceTableIdentifier:      parentSourceTable,
			DestinationTableIdentifier: parentDestTable,
		},
		{
			SourceTableIdentifier:      childSourceTable,
			DestinationTableIdentifier: childDestTable,
		},
	}

	err = s.destConn.SyncIndexesAndTriggers(ctx, tableMappings, s.sourceConn)
	require.NoError(s.t, err)
	childDestTableParsed, err := utils.ParseSchemaTable(childDestTable)
	require.NoError(s.t, err)
	destConstraints, err := s.destConn.getConstraintsForTable(ctx, childDestTableParsed)
	require.NoError(s.t, err)
	fkFound := false
	for _, constraint := range destConstraints {
		if constraint.ConstraintType == "f" && constraint.ConstraintName == "fk_parent" {
			fkFound = true
			require.Contains(s.t, constraint.ConstraintDef, parentDestTable,
				"Foreign key should reference destination parent table")
			break
		}
	}
	require.True(s.t, fkFound, "fk_parent foreign key should be synced")
}

func (s IndexTriggerSyncTestSuite) TestSyncConstraintsTogether() {
	ctx := s.t.Context()
	sourceTable := s.sourceSchema + ".test_constraints_table"
	destTable := s.destSchema + ".test_constraints_table"
	refSourceTable := s.sourceSchema + ".ref_table"
	refDestTable := s.destSchema + ".ref_table"

	_, err := s.sourceConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			code TEXT UNIQUE
		)`, refSourceTable))
	require.NoError(s.t, err)

	_, err = s.destConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			code TEXT UNIQUE
		)`, refDestTable))
	require.NoError(s.t, err)
	_, err = s.sourceConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			name TEXT NOT NULL,
			ref_id INT,
			status TEXT
		)`, sourceTable))
	require.NoError(s.t, err)
	_, err = s.sourceConn.conn.Exec(ctx, fmt.Sprintf(`
		ALTER TABLE %s ADD CONSTRAINT check_name_length 
		CHECK (char_length(name) >= 2)`, sourceTable))
	require.NoError(s.t, err)
	_, err = s.sourceConn.conn.Exec(ctx, fmt.Sprintf(`
		ALTER TABLE %s ADD CONSTRAINT fk_ref 
		FOREIGN KEY (ref_id) REFERENCES %s(id)`, sourceTable, refSourceTable))
	require.NoError(s.t, err)
	_, err = s.destConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			name TEXT NOT NULL,
			ref_id INT,
			status TEXT
		)`, destTable))
	require.NoError(s.t, err)
	tableMappings := []*protos.TableMapping{
		{
			SourceTableIdentifier:      refSourceTable,
			DestinationTableIdentifier: refDestTable,
		},
		{
			SourceTableIdentifier:      sourceTable,
			DestinationTableIdentifier: destTable,
		},
	}

	err = s.destConn.SyncIndexesAndTriggers(ctx, tableMappings, s.sourceConn)
	require.NoError(s.t, err)
	destTableParsed, err := utils.ParseSchemaTable(destTable)
	require.NoError(s.t, err)
	destConstraints, err := s.destConn.getConstraintsForTable(ctx, destTableParsed)
	require.NoError(s.t, err)

	constraintNames := make(map[string]string) // name -> type
	for _, constraint := range destConstraints {
		constraintNames[constraint.ConstraintName] = constraint.ConstraintType
	}

	require.Equal(s.t, "c", constraintNames["check_name_length"], "check_name_length should be synced")
	require.Equal(s.t, "f", constraintNames["fk_ref"], "fk_ref should be synced")
}

func (s IndexTriggerSyncTestSuite) TestSyncAllTogether() {
	ctx := s.t.Context()
	sourceTable := s.sourceSchema + ".test_all_table"
	destTable := s.destSchema + ".test_all_table"
	_, err := s.sourceConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION %s.audit_func()
		RETURNS TRIGGER AS $$
		BEGIN
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql`, s.sourceSchema))
	require.NoError(s.t, err)

	_, err = s.destConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION %s.audit_func()
		RETURNS TRIGGER AS $$
		BEGIN
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql`, s.destSchema))
	require.NoError(s.t, err)
	_, err = s.sourceConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT,
			created_at TIMESTAMPTZ DEFAULT NOW()
		)`, sourceTable))
	require.NoError(s.t, err)
	_, err = s.sourceConn.conn.Exec(ctx, fmt.Sprintf("CREATE INDEX idx_name ON %s(name)", sourceTable))
	require.NoError(s.t, err)
	_, err = s.sourceConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TRIGGER audit_trigger
		AFTER INSERT ON %s
		FOR EACH ROW
		EXECUTE FUNCTION %s.audit_func()`, sourceTable, s.sourceSchema))
	require.NoError(s.t, err)

	// Add check constraint
	_, err = s.sourceConn.conn.Exec(ctx, fmt.Sprintf(`
		ALTER TABLE %s ADD CONSTRAINT check_name_length 
		CHECK (char_length(name) >= 3)`, sourceTable))
	require.NoError(s.t, err)
	_, err = s.destConn.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT,
			created_at TIMESTAMPTZ DEFAULT NOW()
		)`, destTable))
	require.NoError(s.t, err)
	tableMappings := []*protos.TableMapping{
		{
			SourceTableIdentifier:      sourceTable,
			DestinationTableIdentifier: destTable,
		},
	}

	err = s.destConn.SyncIndexesAndTriggers(ctx, tableMappings, s.sourceConn)
	require.NoError(s.t, err)
	destTableParsed, err := utils.ParseSchemaTable(destTable)
	require.NoError(s.t, err)
	destIndexes, err := s.destConn.getIndexesForTable(ctx, destTableParsed)
	require.NoError(s.t, err)

	indexNames := make(map[string]bool)
	for _, idx := range destIndexes {
		indexNames[idx.IndexName] = true
	}
	require.True(s.t, indexNames["idx_name"], "idx_name should be synced")
	destTriggers, err := s.destConn.getTriggersForTable(ctx, destTableParsed)
	require.NoError(s.t, err)
	require.Len(s.t, destTriggers, 1, "Should have 1 trigger")
	require.Equal(s.t, "audit_trigger", destTriggers[0].TriggerName)
	destConstraints, err := s.destConn.getConstraintsForTable(ctx, destTableParsed)
	require.NoError(s.t, err)

	constraintFound := false
	for _, constraint := range destConstraints {
		if constraint.ConstraintName == "check_name_length" && constraint.ConstraintType == "c" {
			constraintFound = true
			break
		}
	}
	require.True(s.t, constraintFound, "check_name_length constraint should be synced")
}

func TestIndexTriggerSync(t *testing.T) {
	e2eshared.RunSuite(t, SetupIndexTriggerSyncSuite)
}
