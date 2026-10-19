//go:build tilt

package connpostgres

import (
	"fmt"
	"testing"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func TestPgCopySnapshotSchemaMismatch(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		alterBefore   bool
		alterAfter    bool
		withoutSlot   bool
		constraint    bool
		wantSQLState  string
		wantPermanent bool
	}{
		{name: "unchanged"},
		{name: "added_before_slot", alterBefore: true, wantSQLState: pgerrcode.UndefinedColumn, wantPermanent: true},
		{name: "added_after_slot", alterAfter: true, wantSQLState: pgerrcode.UndefinedColumn, wantPermanent: true},
		{name: "qrep_without_snapshot", alterBefore: true, withoutSlot: true, wantSQLState: pgerrcode.UndefinedColumn},
		{name: "unrelated_copy_error", constraint: true, wantSQLState: pgerrcode.CheckViolation},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			adminConnector, schema := setupDB(t, "copy_schema_"+tc.name)
			defer adminConnector.Close()
			admin := adminConnector.conn
			defer teardownDB(t, admin, schema)
			srcTable := pgx.Identifier{schema, "source"}.Sanitize()
			dstTable := pgx.Identifier{schema, "destination"}.Sanitize()
			_, err := admin.Exec(ctx, fmt.Sprintf(
				"CREATE TABLE %s (id int PRIMARY KEY, value text); CREATE TABLE %s (LIKE %s); "+
					"INSERT INTO %s VALUES (1, 'snapshot row')", srcTable, dstTable, srcTable, srcTable))
			require.NoError(t, err)
			if tc.constraint {
				_, err = admin.Exec(ctx, "ALTER TABLE "+dstTable+" ADD CHECK (id < 0)")
				require.NoError(t, err)
			}
			alter := func() {
				t.Helper()
				_, err := admin.Exec(ctx, "ALTER TABLE "+srcTable+" ADD COLUMN added_column text DEFAULT 'new value'")
				require.NoError(t, err)
			}
			if tc.alterBefore {
				alter()
			}

			var snapshotName string
			if !tc.withoutSlot {
				replConfig := admin.Config().Copy()
				replConfig.RuntimeParams["replication"] = "database"
				replConn, err := pgx.ConnectConfig(ctx, replConfig)
				require.NoError(t, err)
				defer replConn.Close(ctx)
				slot, err := pglogrepl.ParseCreateReplicationSlot(replConn.PgConn().Exec(ctx,
					"CREATE_REPLICATION_SLOT "+pgx.Identifier{schema}.Sanitize()+" TEMPORARY LOGICAL pgoutput"))
				require.NoError(t, err)
				snapshotName = slot.SnapshotName
			}
			if tc.alterAfter {
				alter()
			}

			pgConfig := internal.GetCatalogPostgresConfigFromEnv(ctx)
			pgConfig.MetadataSchema = &schema
			source, err := NewPostgresConnector(ctx, nil, pgConfig)
			require.NoError(t, err)
			defer source.Close()
			destination, err := NewPostgresConnector(ctx, nil, pgConfig)
			require.NoError(t, err)
			defer destination.Close()
			config := &protos.QRepConfig{
				FlowJobName:                schema,
				WatermarkTable:             schema + ".source",
				DestinationTableIdentifier: schema + ".destination",
				SnapshotName:               snapshotName,
				System:                     protos.TypeSystem_PG,
				Version:                    shared.InternalVersion_Latest,
			}
			require.NoError(t, destination.SetupQRepMetadataTables(ctx, config))
			partition := &protos.QRepPartition{PartitionId: "test", FullTablePartition: true}
			reader, writer := NewPgCopyPipe()
			defer reader.Close()
			pullDone := make(chan error, 1)
			go func() {
				_, _, pullErr := source.PullPgQRepRecords(ctx, shared.CatalogPool{}, nil,
					config, protos.DBType_POSTGRES, partition, writer)
				writer.Close(pullErr)
				pullDone <- pullErr
			}()
			rows, _, syncErr := destination.SyncPgQRepRecords(ctx, config, partition, reader)
			if syncErr != nil {
				writer.HandleQRepSyncError(syncErr)
			}
			pullErr := <-pullDone

			if tc.wantSQLState == "" {
				require.NoError(t, syncErr)
				require.NoError(t, pullErr)
				require.EqualValues(t, 1, rows)
				var value string
				require.NoError(t, admin.QueryRow(ctx, "SELECT value FROM "+dstTable).Scan(&value))
				require.Equal(t, "snapshot row", value)
				return
			}

			var pgErr *pgconn.PgError
			require.ErrorAs(t, syncErr, &pgErr)
			require.Equal(t, tc.wantSQLState, pgErr.Code)
			var appErr *temporal.ApplicationError
			if tc.wantPermanent {
				require.ErrorAs(t, syncErr, &appErr)
				require.True(t, appErr.NonRetryable())
				require.Equal(t, "irrecoverable_snapshot_schema_mismatch", appErr.Type())
				require.ErrorContains(t, syncErr, "align the source and destination schemas")
				require.ErrorContains(t, syncErr, "restart the initial load")
				require.ErrorContains(t, syncErr, dstTable)
				require.ErrorContains(t, syncErr, "added_column")
			} else {
				require.NotErrorAs(t, syncErr, &appErr)
			}
			var count int
			require.NoError(t, admin.QueryRow(ctx, "SELECT count(*) FROM "+dstTable).Scan(&count))
			require.Zero(t, count)
		})
	}
}
