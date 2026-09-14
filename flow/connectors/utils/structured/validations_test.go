package structured

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestSupportedSourcePeer(t *testing.T) {
	require.True(t, SupportedSourcePeer(&protos.Peer{
		Config: &protos.Peer_MongoConfig{MongoConfig: &protos.MongoConfig{}},
	}))
	require.False(t, SupportedSourcePeer(&protos.Peer{
		Config: &protos.Peer_ClickhouseConfig{ClickhouseConfig: &protos.ClickhouseConfig{}},
	}))
	require.False(t, SupportedSourcePeer(&protos.Peer{}))
}

func TestValidateColumns(t *testing.T) {
	require.NoError(t, ValidateColumns("db.coll", []*protos.ColumnSetting{
		{SourceName: "team", DestinationType: "String"},
		{SourceName: "pts", DestinationType: "Nullable(Int64)"},
	}))

	require.ErrorContains(t, ValidateColumns("db.coll", nil),
		"no columns are specified for table db.coll")
	require.ErrorContains(t, ValidateColumns("db.coll", []*protos.ColumnSetting{
		{SourceName: "pts", DestinationType: "Int64 NOT NULL"},
	}), "invalid custom column type")
	require.ErrorContains(t, ValidateColumns("db.coll", []*protos.ColumnSetting{
		{SourceName: "team", DestinationType: "Nullable(String)"},
		{SourceName: "pts"},
	}), "no destination type specified for table db.coll: [pts]")
}
