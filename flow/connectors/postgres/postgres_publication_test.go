package connpostgres

import (
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

func createSchema(t *testing.T, conn *pgx.Conn) string {
	t.Helper()
	schema := "pub_" + strings.ToLower(common.RandomString(8))
	_, err := conn.Exec(t.Context(), "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = conn.Exec(t.Context(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
	})
	return schema
}

func createPublication(t *testing.T, conn *pgx.Conn, pubName string) {
	t.Helper()
	_, err := conn.Exec(t.Context(), "CREATE PUBLICATION "+pubName)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = conn.Exec(t.Context(), "DROP PUBLICATION IF EXISTS "+pubName)
	})
}

func createTableInPublication(t *testing.T, conn *pgx.Conn, schema, table, pubName string) {
	t.Helper()
	ctx := t.Context()
	_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s.%s (id INT PRIMARY KEY)", schema, table))
	require.NoError(t, err)
	_, err = conn.Exec(ctx, fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s.%s", pubName, schema, table))
	require.NoError(t, err)
}

func TestRemoveTablesFromPublication(t *testing.T) {
	t.Parallel()
	connector, err := NewPostgresConnector(t.Context(), nil, internal.GetCatalogPostgresConfigFromEnv(t.Context()))
	require.NoError(t, err)
	t.Cleanup(func() { connector.Close() })

	schema := createSchema(t, connector.conn)
	flowJobName := "remove_pub_" + strings.ToLower(common.RandomString(6))
	pubName := GetDefaultPublicationName(flowJobName)
	createPublication(t, connector.conn, pubName)
	createTableInPublication(t, connector.conn, schema, "stay", pubName)
	createTableInPublication(t, connector.conn, schema, "to_remove", pubName)
	createTableInPublication(t, connector.conn, schema, "to_drop", pubName)

	// Nil/empty input is a no-op
	require.NoError(t, connector.RemoveTablesFromPublication(t.Context(), nil))
	require.NoError(t, connector.RemoveTablesFromPublication(t.Context(), &protos.RemoveTablesFromPublicationInput{}))

	// Table exists in publication get successfully removed
	err = connector.RemoveTablesFromPublication(t.Context(), &protos.RemoveTablesFromPublicationInput{
		FlowJobName: flowJobName,
		TablesToRemove: []*protos.TableMapping{
			{SourceTableIdentifier: schema + ".to_remove"},
		},
	})
	require.NoError(t, err)

	// Table already removed from publication does not error
	err = connector.RemoveTablesFromPublication(t.Context(), &protos.RemoveTablesFromPublicationInput{
		FlowJobName: flowJobName,
		TablesToRemove: []*protos.TableMapping{
			{SourceTableIdentifier: schema + ".to_remove"},
		},
	})
	require.NoError(t, err)

	// Table dropped from database does not error
	_, err = connector.conn.Exec(t.Context(), fmt.Sprintf("DROP TABLE %s.to_drop", schema))
	require.NoError(t, err)
	err = connector.RemoveTablesFromPublication(t.Context(), &protos.RemoveTablesFromPublicationInput{
		FlowJobName: flowJobName,
		TablesToRemove: []*protos.TableMapping{
			{SourceTableIdentifier: schema + ".to_drop"},
		},
	})
	require.NoError(t, err)

	// Verify only "stay" remains
	rows, err := connector.conn.Query(t.Context(),
		"SELECT tablename FROM pg_publication_tables WHERE pubname=$1", pubName)
	require.NoError(t, err)
	remaining, err := pgx.CollectRows[string](rows, pgx.RowTo)
	require.NoError(t, err)
	require.Equal(t, []string{"stay"}, remaining)

	// Verify custom publication skips removal entirely
	customPub := "custom_pub_" + strings.ToLower(common.RandomString(6))
	createPublication(t, connector.conn, customPub)
	createTableInPublication(t, connector.conn, schema, "custom_stay", customPub)
	require.NoError(t, connector.RemoveTablesFromPublication(t.Context(), &protos.RemoveTablesFromPublicationInput{
		FlowJobName:     flowJobName,
		PublicationName: customPub,
		TablesToRemove: []*protos.TableMapping{
			{SourceTableIdentifier: schema + ".custom_stay"},
		},
	}))
	rows, err = connector.conn.Query(t.Context(),
		"SELECT tablename FROM pg_publication_tables WHERE pubname=$1", customPub)
	require.NoError(t, err)
	remaining, err = pgx.CollectRows[string](rows, pgx.RowTo)
	require.NoError(t, err)
	require.Equal(t, []string{"custom_stay"}, remaining)
}
