package conneventhub

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

func TestNewScopedEventhub(t *testing.T) {
	scoped, err := NewScopedEventhub(common.QualifiedTable{Namespace: "ns", Table: "hub.pkcol"})
	require.NoError(t, err)
	assert.Equal(t, ScopedEventhub{NamespaceName: "ns", Eventhub: "hub", PartitionKeyColumn: "pkcol"}, scoped)

	quoted, err := NewScopedEventhub(common.QualifiedTable{Namespace: "ns", Table: `"my-hub"."pk-col"`})
	require.NoError(t, err)
	assert.Equal(t, ScopedEventhub{NamespaceName: "ns", Eventhub: "my-hub", PartitionKeyColumn: "pk-col"}, quoted)

	_, err = NewScopedEventhub(common.QualifiedTable{Table: "hub.pkcol"})
	require.Error(t, err, "missing namespace")
	_, err = NewScopedEventhub(common.QualifiedTable{Namespace: "ns", Table: "hub"})
	require.Error(t, err, "missing partition key column")
	_, err = NewScopedEventhub(common.QualifiedTable{Namespace: "ns", Table: "hub.pk.extra"})
	require.Error(t, err, "more than 3 parts was rejected before QualifiedTable too")
}

// legacy 3-part dotted destinations from Lua scripts keep parsing as before
func TestNewScopedEventhubFromString(t *testing.T) {
	scoped, err := NewScopedEventhubFromString("ns.hub.pkcol")
	require.NoError(t, err)
	assert.Equal(t, ScopedEventhub{NamespaceName: "ns", Eventhub: "hub", PartitionKeyColumn: "pkcol"}, scoped)

	_, err = NewScopedEventhubFromString("ns.hub")
	require.Error(t, err)
}
