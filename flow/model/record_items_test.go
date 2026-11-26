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

// Postgres destinations set NilBytesAsEmpty: an empty binary value (nil Val, as MySQL sources read it) is written as
// an empty value, while a NULL (QValueNull) stays null. Other destinations' options keep nil as null.
func TestRecordItemsNilBytesAsEmpty(t *testing.T) {
	items := NewRecordItems(3)
	items.AddColumn("empty", types.QValueBytes{Val: nil})
	items.AddColumn("null", types.QValueNull(types.QValueKindBytes))
	items.AddColumn("value", types.QValueBytes{Val: []byte("ab")})

	got, err := items.ToJSONWithOptions(ToJSONOptions{NilBytesAsEmpty: true})
	require.NoError(t, err)
	require.JSONEq(t, `{"empty":"","null":null,"value":"YWI="}`, got)

	got, err = items.ToJSONWithOptions(NewToJSONOptions(nil, true))
	require.NoError(t, err)
	require.JSONEq(t, `{"empty":null,"null":null,"value":"YWI="}`, got, "default options are unchanged")
}
