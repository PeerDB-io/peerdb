package internal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestDiffTableColumns(t *testing.T) {
	sortKey := &protos.ColumnSetting{SourceName: "id", Ordering: 1}

	t.Run("returns only new columns", func(t *testing.T) {
		added, err := DiffTableColumns(
			[]string{"id", "name"},
			[]*protos.ColumnSetting{sortKey},
			[]*protos.ColumnSetting{sortKey, {SourceName: "name"}, {SourceName: "age", DestinationName: "user_age"}},
		)
		require.NoError(t, err)
		require.Len(t, added, 1)
		require.Equal(t, "age", added[0].SourceName)
		require.Equal(t, "user_age", added[0].DestinationName)
	})

	t.Run("no new columns", func(t *testing.T) {
		added, err := DiffTableColumns([]string{"id"}, nil, []*protos.ColumnSetting{{SourceName: "id"}})
		require.NoError(t, err)
		require.Empty(t, added)
	})

	t.Run("empty source name", func(t *testing.T) {
		_, err := DiffTableColumns([]string{"id"}, nil, []*protos.ColumnSetting{{SourceName: "id"}, {}})
		require.ErrorContains(t, err, "empty source_name")
	})

	t.Run("duplicate column", func(t *testing.T) {
		_, err := DiffTableColumns([]string{"id"}, nil,
			[]*protos.ColumnSetting{{SourceName: "id"}, {SourceName: "age"}, {SourceName: "age"}})
		require.ErrorContains(t, err, "duplicate column")
	})

	t.Run("removing existing column", func(t *testing.T) {
		_, err := DiffTableColumns([]string{"id", "name"}, nil, []*protos.ColumnSetting{{SourceName: "id"}})
		require.ErrorContains(t, err, `"name" is missing`)
	})

	t.Run("changing settings of existing column", func(t *testing.T) {
		_, err := DiffTableColumns([]string{"id"}, []*protos.ColumnSetting{sortKey},
			[]*protos.ColumnSetting{{SourceName: "id", Ordering: 2}})
		require.ErrorContains(t, err, "changing settings")
	})

	t.Run("adding settings to existing column without prior entry", func(t *testing.T) {
		_, err := DiffTableColumns([]string{"id"}, nil,
			[]*protos.ColumnSetting{{SourceName: "id", DestinationName: "renamed"}})
		require.ErrorContains(t, err, "changing settings")
	})
}
