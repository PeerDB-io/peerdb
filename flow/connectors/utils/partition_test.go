package utils

import (
	"log/slog"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestUUIDToBigIntRoundTrip(t *testing.T) {
	cases := []struct {
		name   string
		uuid   string
		casing HexCasing
	}{
		{"lower case v4", "f47ac10b-58cc-4372-a567-0e02b2c3d479", Lower},
		{"upper case v7", "017F22E2-79B0-7CC3-98C4-DC0C0C07398F", Upper},
		{"zero", "00000000-0000-0000-0000-000000000000", Lower},
		{"max", "ffffffff-ffff-ffff-ffff-ffffffffffff", Lower},
		{"all digits", "01234567-8901-2345-6789-012345678901", Lower},
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
		expectCasing HexCasing
	}{
		{"both lower", "00000000-0000-0000-0000-000000000000", "f47ac10b-58cc-4372-a567-0e02b2c3d479", true, Lower},
		{"both upper", "00000000-0000-0000-0000-000000000000", "F47AC10B-58CC-4372-A567-0E02B2C3D479", true, Upper},
		{"all digits", "01234567-8901-2345-6789-012345678901", "09234567-8901-2345-6789-012345678901", true, Lower},
		{"digits min, upper max", "01234567-8901-2345-6789-012345678901", "F47AC10B-58CC-4372-A567-0E02B2C3D479", true, Upper},
		{"lower min, digits max", "f47ac10b-58cc-4372-a567-0e02b2c3d479", "01234567-8901-2345-6789-012345678901", true, Lower},
		{"mixed case across bounds", "f47ac10b-58cc-4372-a567-0e02b2c3d479", "F47AC10B-58CC-4372-A567-0E02B2C3D479", false, Unknown},
		{"mixed case within a bound", "00000000-0000-0000-0000-000000000000", "F47ac10b-58cc-4372-a567-0e02b2c3d479", false, Unknown},
		{"non-uuid", "apple", "banana", false, Unknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isUUID, hexCasing := DetectUuidWithHexCasing(tc.minVal, tc.maxVal)
			assert.Equal(t, tc.expectIsUUID, isUUID)
			assert.Equal(t, tc.expectCasing, hexCasing)
		})
	}
}

func TestAddPartitionsWithRangeUUID(t *testing.T) {
	cases := []struct {
		name         string
		minV         string
		maxV         string
		expectedCase *regexp.Regexp
	}{
		{"lower", "018f6e7a-1b2c-7def-8a3b-1c2d3e4f5a6b", "f47ac10b-58cc-4372-a567-0e02b2c3d479", UuidLowerRe},
		{"upper", "018F6E7A-1B2C-7DEF-8A3B-1C2D3E4F5A6B", "F47AC10B-58CC-4372-A567-0E02B2C3D479", UuidUpperRe},
	}
	const numPartitions = 32
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := NewPartitionHelper(log.NewStructuredLogger(slog.Default()))
			isUUID, hexCasing := DetectUuidWithHexCasing(tc.minV, tc.maxV)
			require.True(t, isUUID)
			require.NoError(t, p.AddUuidStringPartitionsWithRange(tc.minV, tc.maxV, hexCasing, numPartitions))
			parts := p.GetPartitions()
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

func TestAddPartitionsWithRangeUUIDSinglePartition(t *testing.T) {
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
			p := NewPartitionHelper(log.NewStructuredLogger(slog.Default()))
			isUUID, hexCasing := DetectUuidWithHexCasing(tc.minV, tc.maxV)
			require.True(t, isUUID)
			require.NoError(t, p.AddUuidStringPartitionsWithRange(tc.minV, tc.maxV, hexCasing, tc.numPartitions))
			parts := p.GetPartitions()
			require.Len(t, parts, 1)
			sr, ok := parts[0].Range.Range.(*protos.PartitionRange_StringRange)
			require.True(t, ok)
			assert.Equal(t, tc.minV, sr.StringRange.Start)
			assert.Equal(t, tc.maxV, sr.StringRange.End)
			assert.True(t, sr.StringRange.EndInclusive)
		})
	}
}
