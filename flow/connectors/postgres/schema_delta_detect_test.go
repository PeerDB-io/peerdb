package connpostgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestFindRenameCandidate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prev    []string
		curr    []string
		exclude map[string]struct{}
		want    renameCandidate
		ok      bool
	}{
		{"middle rename", []string{"id", "a", "b"}, []string{"id", "x", "b"}, nil,
			renameCandidate{oldName: "a", newName: "x", leftNeighbor: "id"}, true},
		{"last rename or drop+add", []string{"id", "a"}, []string{"id", "x"}, nil,
			renameCandidate{oldName: "a", newName: "x", leftNeighbor: "id", last: true}, true},
		{"drop middle + add", []string{"id", "a", "b"}, []string{"id", "b", "x"}, nil, renameCandidate{}, false},
		{"plain add", []string{"id"}, []string{"id", "x"}, nil, renameCandidate{}, false},
		{"plain drop", []string{"id", "a"}, []string{"id"}, nil, renameCandidate{}, false},
		{"two renames", []string{"id", "a", "b"}, []string{"id", "x", "y"}, nil, renameCandidate{}, false},
		{"excluded column shifts nothing", []string{"id", "a"}, []string{"id", "secret", "x"},
			map[string]struct{}{"secret": {}}, renameCandidate{oldName: "a", newName: "x", leftNeighbor: "id", last: true}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := findRenameCandidate(tc.prev, tc.curr, tc.exclude)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestRenameCandidateConfirmedBy(t *testing.T) {
	last := renameCandidate{oldName: "a", newName: "x", leftNeighbor: "id", last: true}
	// rename keeps attnum 2
	require.True(t, last.confirmedBy(map[string]int16{"id": 1, "x": 2}, nil))
	// a dropped, x added at attnum 3
	require.False(t, last.confirmedBy(map[string]int16{"id": 1, "x": 3}, []int16{2}))
	// x renamed again since: not confirmable
	require.False(t, last.confirmedBy(map[string]int16{"id": 1}, nil))
	middle := renameCandidate{oldName: "a", newName: "x", leftNeighbor: "id"}
	require.True(t, middle.confirmedBy(map[string]int16{"id": 1, "x": 2, "b": 3}, []int16{5}))
}

func TestIsWideningTypeChange(t *testing.T) {
	numeric := func(precision, scale int32) int32 { return precision<<16 | scale + 4 }
	for _, tc := range []struct {
		from, to     types.QValueKind
		fromTM, toTM int32
		widening     bool
	}{
		{types.QValueKindInt32, types.QValueKindInt64, -1, -1, true},
		{types.QValueKindInt64, types.QValueKindInt32, -1, -1, false},
		{types.QValueKindInt32, types.QValueKindNumeric, -1, -1, true},
		{types.QValueKindFloat32, types.QValueKindFloat64, -1, -1, true},
		{types.QValueKindString, types.QValueKindInt64, -1, -1, false},
		{types.QValueKindTimestamp, types.QValueKindTimestampTZ, -1, -1, false},
		{types.QValueKindNumeric, types.QValueKindNumeric, numeric(10, 2), numeric(20, 4), true},
		{types.QValueKindNumeric, types.QValueKindNumeric, numeric(10, 2), numeric(10, 4), false},
		{types.QValueKindNumeric, types.QValueKindNumeric, numeric(10, 2), -1, true},
		{types.QValueKindNumeric, types.QValueKindNumeric, -1, numeric(10, 2), false},
	} {
		require.Equal(t, tc.widening, isWideningTypeChange(tc.from, tc.to, tc.fromTM, tc.toTM), "%s->%s", tc.from, tc.to)
	}
}
