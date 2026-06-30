package connmysql

import (
	"container/heap"
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

func TestUUIDToBigIntRoundTrip(t *testing.T) {
	cases := []struct {
		name   string
		uuid   string
		casing hexCasing
	}{
		{"lower case v4", "f47ac10b-58cc-4372-a567-0e02b2c3d479", hexCasingLower},
		{"upper case v7", "017F22E2-79B0-7CC3-98C4-DC0C0C07398F", hexCasingUpper},
		{"zero", "00000000-0000-0000-0000-000000000000", hexCasingLower},
		{"max", "ffffffff-ffff-ffff-ffff-ffffffffffff", hexCasingLower},
		{"all digits", "01234567-8901-2345-6789-012345678901", hexCasingLower},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			num, err := uuidToBigInt(tc.uuid)
			require.NoError(t, err)
			uuid, err := bigIntToUUID(num, tc.casing)
			require.NoError(t, err)
			assert.Equal(t, tc.uuid, uuid)
		})
	}
}

func TestUUIDToBigIntInvalid(t *testing.T) {
	for _, s := range []string{
		"",
		"not-a-uuid",
		"zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz",
		"01234567-8901-2345-6789-012345678901a", // 37 chars
	} {
		_, err := uuidToBigInt(s)
		assert.Error(t, err)
	}
}

func TestDetectUUIDCase(t *testing.T) {
	cases := []struct {
		name         string
		minVal       string
		maxVal       string
		expectIsUUID bool
		expectCasing hexCasing
	}{
		{"both lower", "00000000-0000-0000-0000-000000000000", "f47ac10b-58cc-4372-a567-0e02b2c3d479", true, hexCasingLower},
		{"both upper", "00000000-0000-0000-0000-000000000000", "F47AC10B-58CC-4372-A567-0E02B2C3D479", true, hexCasingUpper},
		{"all digits", "01234567-8901-2345-6789-012345678901", "09234567-8901-2345-6789-012345678901", true, hexCasingLower},
		{"digits min, upper max", "01234567-8901-2345-6789-012345678901", "F47AC10B-58CC-4372-A567-0E02B2C3D479", true, hexCasingUpper},
		{"lower min, digits max", "f47ac10b-58cc-4372-a567-0e02b2c3d479", "01234567-8901-2345-6789-012345678901", true, hexCasingLower},
		{"mixed case across bounds", "f47ac10b-58cc-4372-a567-0e02b2c3d479", "F47AC10B-58CC-4372-A567-0E02B2C3D479", false, hexCasingUnknown},
		{"mixed case within a bound", "00000000-0000-0000-0000-000000000000", "F47ac10b-58cc-4372-a567-0e02b2c3d479", false, hexCasingUnknown},
		{"non-uuid", "apple", "banana", false, hexCasingUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isUUID, casing := detectUuidWithHexCasing(tc.minVal, tc.maxVal)
			assert.Equal(t, tc.expectIsUUID, isUUID)
			assert.Equal(t, tc.expectCasing, casing)
		})
	}
}

func TestBuildUuidStringPartitions(t *testing.T) {
	cases := []struct {
		name         string
		minV         string
		maxV         string
		expectedCase *regexp.Regexp
	}{
		{"lower", "018f6e7a-1b2c-7def-8a3b-1c2d3e4f5a6b", "f47ac10b-58cc-4372-a567-0e02b2c3d479", uuidLowerRe},
		{"upper", "018F6E7A-1B2C-7DEF-8A3B-1C2D3E4F5A6B", "F47AC10B-58CC-4372-A567-0E02B2C3D479", uuidUpperRe},
	}
	const numPartitions = 32
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isUUID, casing := detectUuidWithHexCasing(tc.minV, tc.maxV)
			require.True(t, isUUID)
			parts, err := buildUuidStringPartitions(tc.minV, tc.maxV, casing, numPartitions)
			require.NoError(t, err)
			require.Len(t, parts, numPartitions)

			prevEnd := ""
			for i, part := range parts {
				r, ok := part.Range.Range.(*protos.PartitionRange_StringRange)
				require.True(t, ok)
				sr := r.StringRange
				if i == 0 {
					assert.Equal(t, tc.minV, sr.Start)
				} else {
					assert.Equal(t, prevEnd, sr.Start)
					assert.Regexp(t, tc.expectedCase, sr.Start)
				}
				startInt, err := uuidToBigInt(sr.Start)
				require.NoError(t, err)
				endInt, err := uuidToBigInt(sr.End)
				require.NoError(t, err)
				assert.Negative(t, startInt.Cmp(endInt))

				isLast := i == len(parts)-1
				assert.Equal(t, isLast, sr.EndInclusive)
				if isLast {
					assert.Equal(t, tc.maxV, sr.End)
				}
				prevEnd = sr.End
			}
		})
	}
}

func TestBuildUuidStringPartitionsSinglePartition(t *testing.T) {
	uuid1 := "018f6e7a-1b2c-7def-8a3b-1c2d3e4f5a6b"
	uuid2 := "f47ac10b-58cc-4372-a567-0e02b2c3d479"
	cases := []struct {
		name          string
		minV          string
		maxV          string
		numPartitions int64
	}{
		{"one partition requested", uuid1, uuid2, 1},
		{"min equals max", uuid1, uuid1, 8},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isUUID, casing := detectUuidWithHexCasing(tc.minV, tc.maxV)
			require.True(t, isUUID)
			parts, err := buildUuidStringPartitions(tc.minV, tc.maxV, casing, tc.numPartitions)
			require.NoError(t, err)
			require.Len(t, parts, 1)
			sr, ok := parts[0].Range.Range.(*protos.PartitionRange_StringRange)
			require.True(t, ok)
			assert.Equal(t, tc.minV, sr.StringRange.Start)
			assert.Equal(t, tc.maxV, sr.StringRange.End)
			assert.True(t, sr.StringRange.EndInclusive)
		})
	}
}

func TestBuildUuidStringPartitionsInvalid(t *testing.T) {
	cases := []struct {
		name string
		minV string
		maxV string
	}{
		{"arbitrary string", "apple", "banana"},
		{"inverted", "f47ac10b-58cc-4372-a567-0e02b2c3d479", "018f6e7a-1b2c-7def-8a3b-1c2d3e4f5a6b"},
		{"too short", "018f6e7a-1b2c", "f47ac10b-58cc"},
		{"too long", "018f6e7a-1b2c-7def-8a3b-1c2d3e4f5a6bb", "f47ac10b-58cc-4372-a567-0e02b2c3d4799"},
	}
	const numPartitions = 32
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := buildUuidStringPartitions(tc.minV, tc.maxV, hexCasingLower, numPartitions)
			require.Error(t, err)
		})
	}
}

func TestStringMidpoint(t *testing.T) {
	cases := []struct {
		name     string
		s1       string
		s2       string
		expected string
	}{
		{"identical", "abc", "abc", "abc"},
		{"empty bounds", "", "", ""},
		{"with spaces", " m e", " o ghi", " n fDDOOO"},
		{"numeric string", "111", "999", "555"},
		{"with shared prefix", "prefix-hello", "prefix-world", "prefix-p;@=:"},
		{"max length", "qwertyuiop", "zxcvbnmlkj", "vHdtktB;"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mid := stringMidpoint(tc.s1, tc.s2)
			assert.Equal(t, tc.expected, mid)
			assert.LessOrEqual(t, tc.s1, mid)
			assert.LessOrEqual(t, mid, tc.s2)
		})
	}
}

func TestBase95RoundTrip(t *testing.T) {
	trimRightSpaces := func(s string) string {
		return regexp.MustCompile(` +$`).ReplaceAllString(s, "")
	}

	for _, s := range []string{"", "a", "ant", "cat", "~~~~~~~~", "555", "Mixed42!", "longer_than_8_characters"} {
		decoded := base95IntegerToString(stringToBase95Integer(s))
		expected := s
		if len(expected) > 8 {
			expected = expected[0:8]
		}
		assert.Equal(t, expected, trimRightSpaces(decoded))
	}
}

func TestStringPartitionHeapPopsLargest(t *testing.T) {
	h := &stringPartitionHeap{}
	heap.Init(h)
	for _, rows := range []uint64{5, 100, 1, 42, 7} {
		heap.Push(h, stringPartitionEntry{rows: rows})
	}
	var popped []uint64
	for h.Len() > 0 {
		popped = append(popped, heap.Pop(h).(stringPartitionEntry).rows)
	}
	assert.Equal(t, []uint64{100, 42, 7, 5, 1}, popped)
}

type fakeRangeProber struct {
	keys []string
	less func(a string, b string) bool
}

func newFakeRangeProber(keys []string, less func(a string, b string) bool) *fakeRangeProber {
	sorted := slices.Clone(keys)
	slices.SortFunc(sorted, func(a string, b string) int {
		switch {
		case less(a, b):
			return -1
		case less(b, a):
			return 1
		default:
			return 0
		}
	})
	return &fakeRangeProber{keys: sorted, less: less}
}

func (f *fakeRangeProber) estimateRowsInRange(
	_ context.Context, _ string, _ string, start string, end string,
) (uint64, error) {
	var n uint64
	for _, k := range f.keys {
		if !f.less(k, start) && f.less(k, end) { // start <= k < end
			n++
		}
	}
	return n, nil
}

func (f *fakeRangeProber) fetchNextRealKey(
	_ context.Context, _ string, _ string, midpoint string, start string, end string,
) (string, bool, error) {
	for _, k := range f.keys {
		if f.less(k, midpoint) {
			continue
		}
		if !f.less(start, k) { // k <= start
			continue
		}
		if !f.less(k, end) { // k >= end
			continue
		}
		return k, true, nil
	}
	return "", false, nil
}

// binaryLess mimics a *_bin collation
func binaryLess(a string, b string) bool { return a < b }

// caseInsensitiveLess mimics a *_ci collation
func caseInsensitiveLess(a string, b string) bool {
	la, lb := strings.ToLower(a), strings.ToLower(b)
	if la != lb {
		return la < lb
	}
	return a < b
}

func runAdaptiveStringPartitions(t *testing.T, keys []string, less func(a string, b string) bool, numPartitions int64) []*protos.QRepPartition {
	t.Helper()
	planner := newFakeRangeProber(keys, less)
	minVal, maxVal := planner.keys[0], planner.keys[len(planner.keys)-1]
	table := &common.QualifiedTable{Namespace: "db", Table: "t"}
	partitions, err := buildAdaptiveStringPartitions(
		t.Context(), planner, log.NewStructuredLogger(slog.Default()),
		table, "id", minVal, maxVal, numPartitions)
	require.NoError(t, err)
	return partitions
}

func verifyFullCoverage(t *testing.T, partitions []*protos.QRepPartition, keys []string, less func(a string, b string) bool) {
	t.Helper()
	require.NotEmpty(t, partitions)

	// verify one end-inclusive partition exists
	count := 0
	for _, p := range partitions {
		if p.GetRange().GetStringRange().EndInclusive {
			count++
		}
	}
	require.Equal(t, 1, count)

	// verify each key belongs to exactly one partition
	for _, key := range keys {
		matched := -1
		for i, p := range partitions {
			sr := p.GetRange().GetStringRange()
			require.NotNil(t, sr)
			var inRange bool
			if sr.EndInclusive {
				inRange = !less(key, sr.Start) && !less(sr.End, key) // start <= key <= end
			} else {
				inRange = !less(key, sr.Start) && less(key, sr.End) // start <= key < end
			}
			if inRange {
				require.Equal(t, -1, matched)
				matched = i
			}
		}
		require.NotEqual(t, -1, matched)
	}
}

func TestBuildAdaptiveStringPartitions_WellDistributed(t *testing.T) {
	var keys []string
	for c := byte('a'); c <= byte('z'); c++ {
		keys = append(keys, fmt.Sprintf("%c_value", c))
	}
	const maxNumPartitions = 8
	partitions := runAdaptiveStringPartitions(t, keys, binaryLess, maxNumPartitions)
	require.Len(t, partitions, maxNumPartitions)
	verifyFullCoverage(t, partitions, keys, binaryLess)
}

func TestBuildAdaptiveStringPartitions_NumKeysLessThanNumPartitions(t *testing.T) {
	keys := []string{"alpha", "bravo", "charlie", "delta"}
	partitions := runAdaptiveStringPartitions(t, keys, binaryLess, 8)
	// [alpha, bravo), [bravo, charlie), [charlie, delta]
	require.Len(t, partitions, 3)
	verifyFullCoverage(t, partitions, keys, binaryLess)
}

func TestBuildAdaptiveStringPartitions_LowCardinality(t *testing.T) {
	keys := []string{"aaa", "aaa", "aaa", "ooo", "ooo", "zzz", "zzz", "zzz", "zzz"}
	distinct := len(slices.Compact(slices.Clone(keys)))
	partitions := runAdaptiveStringPartitions(t, keys, binaryLess, 8)
	require.LessOrEqual(t, len(partitions), distinct-1)
	require.GreaterOrEqual(t, len(partitions), 1)
	verifyFullCoverage(t, partitions, keys, binaryLess)
}

func TestBuildAdaptiveStringPartitions_AllIdentical(t *testing.T) {
	keys := []string{"same", "same", "same", "same"}
	partitions := runAdaptiveStringPartitions(t, keys, binaryLess, 8)
	require.Len(t, partitions, 1)
	verifyFullCoverage(t, partitions, keys, binaryLess)
}

func TestBuildAdaptiveStringPartitions_Unicode(t *testing.T) {
	keys := []string{
		"αlpha", "βeta", "γamma", "日本語", "中文", "🍕pizza", "🍟fries", "smiley😀", "café", "naïve",
	}
	partitions := runAdaptiveStringPartitions(t, keys, binaryLess, 8)
	verifyFullCoverage(t, partitions, keys, binaryLess)
}

func TestBuildAdaptiveStringPartitions_CaseInsensitiveCollation(t *testing.T) {
	keys := []string{
		"Apple", "apple", "BANANA", "banana", "Cherry", "cherry", "Date", "DATE", "elderberry", "Fig",
	}
	partitions := runAdaptiveStringPartitions(t, keys, caseInsensitiveLess, 8)
	verifyFullCoverage(t, partitions, keys, caseInsensitiveLess)
}

func TestBuildAdaptiveStringPartitions_NarrowAlphabetLimitation(t *testing.T) {
	keys := make([]string, 0, 100)
	for i := 1; i <= 100; i++ {
		keys = append(keys, fmt.Sprintf("key_%04d", i))
	}

	// demonstrate midpoint is outside min/max range so fetchNextRealKey always returns false
	require.Equal(t, "key_00_`", stringMidpoint("key_0001", "key_0100"))

	partitions := runAdaptiveStringPartitions(t, keys, binaryLess, 8)
	require.Len(t, partitions, 1)
	verifyFullCoverage(t, partitions, keys, binaryLess)
}

func TestBuildAdaptiveStringPartitions_MixedCase_UUID(t *testing.T) {
	keys := make([]string, 0, 100)
	for i := 1; i <= 100; i++ {
		id, err := uuid.NewUUID()
		require.NoError(t, err)
		if i%2 == 0 {
			keys = append(keys, id.String())
		} else {
			keys = append(keys, strings.ToUpper(id.String()))
		}
	}
	const numPartitions = 5
	partitions := runAdaptiveStringPartitions(t, keys, binaryLess, numPartitions)
	require.Len(t, partitions, numPartitions)
	verifyFullCoverage(t, partitions, keys, binaryLess)
}
