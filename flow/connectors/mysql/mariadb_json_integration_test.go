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

// TestMariaDBJSONColumnsTypedAsJSON checks that schema discovery types a MariaDB column declared JSON, which
// the server stores as LONGTEXT with a json_valid check, as JSON, along with a LONGTEXT that carries the same
// check by hand, while a plain LONGTEXT stays a string and mirrors below the version keep the old typing.
func TestMariaDBJSONColumnsTypedAsJSON(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	connector := newTestConnector(t, ctx)
	if connector.config.Flavor != protos.MySqlFlavor_MYSQL_MARIA {
		t.Skip("MariaDB only; MySQL has a JSON type that needs no check constraint")
	}

	db := testDBName("mariajson")
	createTestDB(t, ctx, connector, db)
	table := db + ".jt"
	_, err := connector.Execute(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id INT PRIMARY KEY, j JSON, k LONGTEXT CHECK (json_valid(k)), "+
			"t LONGTEXT, b LONGBLOB)", table))
	require.NoError(t, err)

	kindsAt := func(version uint32) map[string]types.QValueKind {
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

	kinds := kindsAt(shared.InternalVersion_Latest)
	require.Equal(t, types.QValueKindJSON, kinds["j"])
	require.Equal(t, types.QValueKindJSON, kinds["k"])
	require.Equal(t, types.QValueKindString, kinds["t"])
	require.Equal(t, types.QValueKindBytes, kinds["b"])

	// a mirror created before the change keeps typing the JSON column as a string
	old := kindsAt(shared.InternalVersion_MariaDBJSONColumns - 1)
	require.Equal(t, types.QValueKindString, old["j"])
	require.Equal(t, types.QValueKindString, old["k"])

	// the column listing the UI uses agrees with the schema
	columns, err := connector.GetColumns(ctx, shared.InternalVersion_Latest, db, "jt")
	require.NoError(t, err)
	listed := make(map[string]string)
	for _, column := range columns.Columns {
		listed[column.Name] = column.Qkind
	}
	require.Equal(t, string(types.QValueKindJSON), listed["j"])
	require.Equal(t, string(types.QValueKindJSON), listed["k"])
	require.Equal(t, string(types.QValueKindString), listed["t"])
}
