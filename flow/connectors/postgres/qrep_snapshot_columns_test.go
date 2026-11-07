//go:build tilt

package connpostgres

import (
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// snapshotColumnsConnectors returns a connector to sync into the schema of setupDB,
// and the name of a snapshot exported until the test ends
func snapshotColumnsConnectors(t *testing.T, schemaName string) (*PostgresConnector, string) {
	t.Helper()

	dst, err := NewPostgresConnector(t.Context(), nil, internal.GetCatalogPostgresConfigFromEnv(t.Context()))
	require.NoError(t, err)
	t.Cleanup(func() { dst.Close() })
	dst.metadataSchema = schemaName
	require.NoError(t, dst.SetupQRepMetadataTables(t.Context(), &protos.QRepConfig{}))

	exporter, err := NewPostgresConnector(t.Context(), nil, internal.GetCatalogPostgresConfigFromEnv(t.Context()))
	require.NoError(t, err)
	t.Cleanup(func() { exporter.Close() })
	snapshot, tx, err := exporter.ExportTxSnapshot(t.Context(), "", nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, snapshot.SnapshotName)
	t.Cleanup(func() { require.NoError(t, exporter.FinishExport(tx)) })

	return dst, snapshot.SnapshotName
}

func pullAndSyncPgPartition(
	t *testing.T,
	src *PostgresConnector,
	dst *PostgresConnector,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
) {
	t.Helper()

	read, write := NewPgCopyPipe()
	pullErr := make(chan error, 1)
	go func() {
		_, _, err := src.PullPgQRepRecords(t.Context(), shared.CatalogPool{}, nil, config, protos.DBType_POSTGRES, partition, write)
		write.Close(err)
		pullErr <- err
	}()
	_, _, syncErr := dst.SyncPgQRepRecords(t.Context(), config, partition, read)
	if syncErr != nil {
		write.HandleQRepSyncError(syncErr)
	}
	require.NoError(t, syncErr, "partition %s", partition.PartitionId)
	require.NoError(t, <-pullErr, "partition %s", partition.PartitionId)
}

func requireTableRows(t *testing.T, conn *pgx.Conn, query string, expected []string) {
	t.Helper()

	rows, err := conn.Query(t.Context(), query)
	require.NoError(t, err)
	actual, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestPullSnapshotColumns(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	src, schemaName := setupDB(t, "snapshot_columns")
	conn := src.conn
	defer src.Close()
	defer teardownDB(t, conn, schemaName)

	schema := common.QuoteIdentifier(schemaName)
	_, err := conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %[1]s."Src" (id INT PRIMARY KEY, wm INT, "Mixed Case" TEXT, secret TEXT, dropped TEXT);
		INSERT INTO %[1]s."Src" VALUES (1, 1, 'm1', 's1', 'd1'), (2, 2, 'm2', 's2', 'd2'), (3, NULL, 'm3', 's3', 'd3');
		CREATE TABLE %[1]s.full_dst (id INT PRIMARY KEY, wm INT, other_src TEXT, "Mixed Case" TEXT, dropped TEXT);
		CREATE TABLE %[1]s.ranged_dst (id INT PRIMARY KEY, wm INT, other_src TEXT, "Mixed Case" TEXT, dropped TEXT);`, schema))
	require.NoError(t, err)

	dst, snapshotName := snapshotColumnsConnectors(t, schemaName)

	// after the snapshot columns were chosen: a column is added, which destinations lack,
	// and one of the snapshot columns is dropped; changes to rows aren't in the snapshot
	_, err = conn.Exec(ctx, fmt.Sprintf(`
		ALTER TABLE %[1]s."Src" ADD COLUMN added_later TEXT DEFAULT 'later', DROP COLUMN dropped;
		UPDATE %[1]s."Src" SET "Mixed Case" = 'changed' WHERE id = 1;
		INSERT INTO %[1]s."Src" (id, wm, "Mixed Case") VALUES (4, 4, 'm4');`, schema))
	require.NoError(t, err)

	config := &protos.QRepConfig{
		FlowJobName:     "test_snapshot_columns",
		WatermarkTable:  schemaName + ".Src",
		WatermarkColumn: "wm",
		SnapshotName:    snapshotName,
		System:          protos.TypeSystem_PG,
		Version:         shared.InternalVersion_Latest,
		Exclude:         []string{"secret"},
		// columns of the destination schema, which has a column of another source table sharing it
		SelectedColumns: []string{"id", "wm", "other_src", "Mixed Case", "dropped"},
	}
	// snapshot values, without those of the dropped column
	expected := []string{"1|1|null|m1|null", "2|2|null|m2|null", "3|null|null|m3|null"}
	rowsQuery := `SELECT concat_ws('|', id, coalesce(wm::text, 'null'), coalesce(other_src, 'null'), "Mixed Case",
		coalesce(dropped, 'null')) FROM %s.%s ORDER BY id`

	config.DestinationTableIdentifier = schemaName + ".full_dst"
	pullAndSyncPgPartition(t, src, dst, config, &protos.QRepPartition{PartitionId: "full", FullTablePartition: true})
	requireTableRows(t, conn, fmt.Sprintf(rowsQuery, schema, "full_dst"), expected)

	config.DestinationTableIdentifier = schemaName + ".ranged_dst"
	pullAndSyncPgPartition(t, src, dst, config, &protos.QRepPartition{
		PartitionId: "range",
		Range:       &protos.PartitionRange{Range: &protos.PartitionRange_IntRange{IntRange: &protos.IntPartitionRange{Start: 1, End: 10}}},
	})
	pullAndSyncPgPartition(t, src, dst, config, &protos.QRepPartition{
		PartitionId: "null",
		Range:       &protos.PartitionRange{Range: &protos.PartitionRange_NullRange{NullRange: &protos.NullPartitionRange{}}},
	})
	requireTableRows(t, conn, fmt.Sprintf(rowsQuery, schema, "ranged_dst"), expected)
}

func TestPullSnapshotColumnsChildTables(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	src, schemaName := setupDB(t, "snapshot_columns_children")
	conn := src.conn
	defer src.Close()
	defer teardownDB(t, conn, schemaName)

	schema := common.QuoteIdentifier(schemaName)
	_, err := conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %[1]s."Parent" (id INT, "Region" INT, val TEXT, secret TEXT, PRIMARY KEY ("Region", id))
			PARTITION BY RANGE ("Region");
		CREATE TABLE %[1]s."Child_0" PARTITION OF %[1]s."Parent" FOR VALUES FROM (0) TO (10);
		CREATE TABLE %[1]s."Child_1" PARTITION OF %[1]s."Parent" FOR VALUES FROM (10) TO (20);
		INSERT INTO %[1]s."Parent" VALUES (1, 0, 'v1', 's1'), (2, 5, 'v2', 's2'), (3, 10, 'v3', 's3'), (4, 15, 'v4', 's4');
		CREATE TABLE %[1]s."Parent_Dst" (id INT, "Region" INT, val TEXT, PRIMARY KEY ("Region", id));`, schema))
	require.NoError(t, err)

	dst, snapshotName := snapshotColumnsConnectors(t, schemaName)

	_, err = conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s."Parent" ADD COLUMN added_later TEXT DEFAULT 'later'`, schema))
	require.NoError(t, err)

	pullAndSyncPgPartition(t, src, dst, &protos.QRepConfig{
		FlowJobName:                "test_snapshot_columns_children",
		WatermarkTable:             schemaName + ".Parent",
		WatermarkColumn:            "ctid",
		SnapshotName:               snapshotName,
		System:                     protos.TypeSystem_PG,
		Version:                    shared.InternalVersion_Latest,
		Exclude:                    []string{"secret"},
		SelectedColumns:            []string{"id", "Region", "val"},
		DestinationTableIdentifier: schemaName + ".Parent_Dst",
	}, &protos.QRepPartition{
		PartitionId: "children",
		ChildTableRanges: []*protos.ChildTableRange{
			{Table: schemaName + ".Child_0", Start: 0, End: 1000},
			{Table: schemaName + ".Child_1", Start: 0, End: 1000},
		},
	})
	requireTableRows(t, conn, fmt.Sprintf(
		`SELECT concat_ws('|', id, "Region", val) FROM %s."Parent_Dst" ORDER BY id`, schema),
		[]string{"1|0|v1", "2|5|v2", "3|10|v3", "4|15|v4"})
}
