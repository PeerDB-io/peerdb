//go:build tilt

package connpostgres

import (
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// A parent with no children when the startup map is built is missing from idToRelKindMap. Runtime discovery of a
// child created afterwards has to record the parent's relkind, or the child's rows would carry the parent's schema.
func TestRuntimeDiscoveredChildStampsOwnSchema(t *testing.T) {
	t.Parallel()
	connector, schemaName := setupDB(t, "stamp_late")
	tenant := schemaName + "_tenant"
	defer connector.Close()
	defer teardownDB(t, connector.conn, schemaName)
	defer teardownDB(t, connector.conn, tenant)

	parent := common.QuoteIdentifier(schemaName) + ".parent"
	_, err := connector.conn.Exec(t.Context(), "CREATE TABLE "+parent+" (id INT PRIMARY KEY); CREATE SCHEMA "+
		common.QuoteIdentifier(tenant))
	require.NoError(t, err)
	var parentRelID uint32
	require.NoError(t, connector.conn.QueryRow(t.Context(), "SELECT $1::regclass::oid", parent).Scan(&parentRelID))

	mirrored := []uint32{parentRelID}
	childToParent, relKinds, err := getChildToParentRelIDMap(t.Context(), connector.conn, mirrored, true)
	require.NoError(t, err)
	require.NotContains(t, relKinds, parentRelID, "the parent has no children yet")
	p := &PostgresCDCSource{
		PostgresConnector:                        connector,
		srcTableIDNameMapping:                    map[uint32]string{parentRelID: schemaName + ".parent"},
		schemaNameForRelID:                       map[uint32]string{},
		childToParentRelIDMapping:                childToParent,
		idToRelKindMap:                           relKinds,
		mirroredRelIDs:                           mirrored,
		hushWarnUnknownTableDetected:             map[uint32]struct{}{},
		handleInheritanceForNonPartitionedTables: true,
		internalVersion:                          shared.InternalVersion_SourceSchemaFromInheritanceChild,
	}

	child := common.QuoteIdentifier(tenant) + ".parent"
	_, err = connector.conn.Exec(t.Context(), "CREATE TABLE "+child+" () INHERITS ("+parent+")")
	require.NoError(t, err)
	var childRelID uint32
	require.NoError(t, connector.conn.QueryRow(t.Context(), "SELECT $1::regclass::oid", child).Scan(&childRelID))

	mappedRelID, relKind, err := p.checkIfUnknownTableInherits(t.Context(), childRelID)
	require.NoError(t, err)
	require.Equal(t, parentRelID, mappedRelID)
	require.Equal(t, byte('r'), relKind)
	schema, err := p.getSourceSchemaForDestinationColumn(
		&pglogrepl.RelationMessage{RelationID: childRelID, Namespace: tenant, RelationName: "parent"},
		mappedRelID, schemaName+".parent")
	require.NoError(t, err)
	require.Equal(t, tenant, schema)
}
