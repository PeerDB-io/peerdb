package model

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestRecordItemsClearValuesOverBytes(t *testing.T) {
	items := NewRecordItems(4)
	items.AddColumn("at_limit", types.QValueString{Val: "12345"})
	items.AddColumn("over_limit", types.QValueString{Val: "123456"})
	items.AddColumn("json", types.QValueJSON{Val: `{"key":"value"}`})
	items.AddColumn("hstore", types.QValueHStore{Val: `"key"=>"value"`})

	opts := NewToJSONOptions(nil, true)
	opts.ClearValuesOverBytes = 5
	got, err := items.toMap(opts)
	require.NoError(t, err)
	require.Equal(t, "12345", got["at_limit"])
	require.Empty(t, got["over_limit"])
	require.Equal(t, "{}", got["json"])
	require.Empty(t, got["hstore"])
}

func TestRecordItemsPreserveValuesByDefault(t *testing.T) {
	items := NewRecordItems(3)
	items.AddColumn("string", types.QValueString{Val: "123456"})
	items.AddColumn("json", types.QValueJSON{Val: `{"key":"value"}`})
	items.AddColumn("hstore", types.QValueHStore{Val: `"key"=>"value"`})

	got, err := items.toMap(NewToJSONOptions(nil, true))
	require.NoError(t, err)
	require.Equal(t, "123456", got["string"])
	require.JSONEq(t, `{"key":"value"}`, got["json"].(string))
	require.JSONEq(t, `{"key":"value"}`, got["hstore"].(string))
}
