//go:build tilt

package connmysql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// TestMariaDBJSONColumnsTypedAsJSON checks schema discovery against a real MariaDB table. MariaDB stores a
// column declared JSON as a LONGTEXT with a json_valid check, and discovery should type exactly those
// columns as JSON, on new mirrors only.
func TestMariaDBJSONColumnsTypedAsJSON(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	connector := newTestConnector(t, ctx)
	if connector.config.Flavor != protos.MySqlFlavor_MYSQL_MARIA {
		t.Skip("MariaDB only; MySQL has a real JSON type")
	}

	db := testDBName("mariajson")
	createTestDB(t, ctx, connector, db)
	table := db + ".json_columns"
	_, err := connector.Execute(ctx, fmt.Sprintf(`CREATE TABLE %s (
		id               INT PRIMARY KEY,
		declared_json    JSON,
		checked_longtext LONGTEXT CHECK (json_valid(checked_longtext)),
		checked_varchar  VARCHAR(100) CHECK (json_valid(checked_varchar)),
		plain_longtext   LONGTEXT,
		plain_blob       LONGBLOB)`, table))
	require.NoError(t, err)

	// the type each column should get on a new mirror
	want := map[string]types.QValueKind{
		"id":               types.QValueKindInt32,
		"declared_json":    types.QValueKindJSON,   // MariaDB turns it into a LONGTEXT with a json_valid check
		"checked_longtext": types.QValueKindJSON,   // the same column written by hand
		"checked_varchar":  types.QValueKindString, // only LONGTEXT, which MariaDB's JSON alias creates, is promoted
		"plain_longtext":   types.QValueKindString, // no check
		"plain_blob":       types.QValueKindBytes,
	}

	discovered := func(t *testing.T, version uint32) map[string]types.QValueKind {
		t.Helper()
		schemas, err := connector.GetTableSchema(ctx, nil, version, protos.TypeSystem_Q,
			[]*protos.TableMapping{{SourceTableIdentifier: table}})
		require.NoError(t, err)
		kinds := make(map[string]types.QValueKind)
		for _, column := range schemas[table].Columns {
			kinds[column.Name] = types.QValueKind(column.Type)
		}
		return kinds
	}

	t.Run("new mirror", func(t *testing.T) {
		require.Equal(t, want, discovered(t, shared.InternalVersion_Latest))
	})

	t.Run("mirror created before the change keeps String", func(t *testing.T) {
		kinds := discovered(t, shared.InternalVersion_MariaDBJSONColumns-1)
		require.Equal(t, types.QValueKindString, kinds["declared_json"])
		require.Equal(t, types.QValueKindString, kinds["checked_longtext"])
	})

	t.Run("UI column listing agrees", func(t *testing.T) {
		columns, err := connector.GetColumns(ctx, shared.InternalVersion_Latest, db, "json_columns")
		require.NoError(t, err)
		listed := make(map[string]types.QValueKind)
		for _, column := range columns.Columns {
			listed[column.Name] = types.QValueKind(column.Qkind)
		}
		require.Equal(t, want, listed)
	})
}
