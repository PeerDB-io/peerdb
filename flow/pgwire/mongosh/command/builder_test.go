package command

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestUpsert(t *testing.T) {
	tests := []struct {
		name     string
		doc      bson.D
		key      string
		value    interface{}
		wantLen  int
		checkKey string
	}{
		{
			name:     "insert new key",
			doc:      bson.D{{Key: "a", Value: 1}},
			key:      "b",
			value:    2,
			wantLen:  2,
			checkKey: "b",
		},
		{
			name:     "update existing key",
			doc:      bson.D{{Key: "a", Value: 1}, {Key: "b", Value: 2}},
			key:      "b",
			value:    3,
			wantLen:  2,
			checkKey: "b",
		},
		{
			name:     "insert into empty doc",
			doc:      bson.D{},
			key:      "a",
			value:    1,
			wantLen:  1,
			checkKey: "a",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Upsert(tt.doc, tt.key, tt.value)
			require.Len(t, got, tt.wantLen)

			// Check the value was set correctly
			found := false
			for _, elem := range got {
				if elem.Key == tt.checkKey {
					found = true
					require.Equal(t, tt.value, elem.Value, "Value for key %s", tt.checkKey)
					break
				}
			}
			require.True(t, found, "Key %s not found in result", tt.checkKey)
		})
	}
}

func TestExtractField(t *testing.T) {
	doc := bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{}},
		{Key: "limit", Value: 10},
	}

	// Test existing field
	val, ok := extractField(doc, "find")
	require.True(t, ok, "extractField() failed to find 'find' key")
	require.Equal(t, "users", val)

	// Test non-existing field
	_, ok = extractField(doc, "notfound")
	require.False(t, ok, "extractField() should return false for non-existing key")
}
