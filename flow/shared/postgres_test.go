package shared

import (
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func TestNewPostgresTypeMap(t *testing.T) {
	originalLocal := time.Local
	time.Local = time.FixedZone("UTC-8", -8*60*60)
	t.Cleanup(func() { time.Local = originalLocal })

	typeMap := NewPostgresTypeMap()

	for _, timestamp := range []struct {
		name                                   string
		raw                                    string
		oid, arrayOID, rangeOID, rangeArrayOID uint32
		multirangeOID, multirangeArrayOID      uint32
		want                                   time.Time
	}{
		{
			"timestamp", "2024-01-02 03:04:05",
			pgtype.TimestampOID, pgtype.TimestampArrayOID, pgtype.TsrangeOID, pgtype.TsrangeArrayOID,
			pgtype.TsmultirangeOID, pgtype.TsmultirangeArrayOID,
			time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC),
		},
		{
			"timestamptz", "2024-01-02 03:04:05+05:30",
			pgtype.TimestamptzOID, pgtype.TimestamptzArrayOID, pgtype.TstzrangeOID, pgtype.TstzrangeArrayOID,
			pgtype.TstzmultirangeOID, pgtype.TstzmultirangeArrayOID,
			time.Date(2024, 1, 1, 21, 34, 5, 0, time.UTC),
		},
	} {
		rangeText := fmt.Sprintf("[%q,%q]", timestamp.raw, timestamp.raw)
		multirangeText := "{" + rangeText + "}"
		rangeValue := pgtype.Range[time.Time]{
			Lower: timestamp.want, Upper: timestamp.want,
			LowerType: pgtype.Inclusive, UpperType: pgtype.Inclusive, Valid: true,
		}
		multirangeValue := pgtype.Multirange[pgtype.Range[time.Time]]{rangeValue}
		for _, tc := range []struct {
			name        string
			oid         uint32
			raw         string
			binaryValue any
		}{
			{"scalar", timestamp.oid, timestamp.raw, timestamp.want},
			{"array", timestamp.arrayOID, fmt.Sprintf("{%q}", timestamp.raw), []time.Time{timestamp.want}},
			{"range", timestamp.rangeOID, rangeText, rangeValue},
			{"range array", timestamp.rangeArrayOID, fmt.Sprintf("{%q}", rangeText), []pgtype.Range[time.Time]{rangeValue}},
			{"multirange", timestamp.multirangeOID, multirangeText, multirangeValue},
		} {
			t.Run(timestamp.name+"/"+tc.name, func(t *testing.T) {
				typ, ok := typeMap.TypeForOID(tc.oid)
				require.True(t, ok)
				value, err := typ.Codec.DecodeValue(typeMap, tc.oid, pgtype.TextFormatCode, []byte(tc.raw))
				require.NoError(t, err)
				requireUTCTimestamps(t, value, timestamp.want)

				binary, err := typeMap.Encode(tc.oid, pgtype.BinaryFormatCode, tc.binaryValue, nil)
				require.NoError(t, err)
				value, err = typ.Codec.DecodeValue(typeMap, tc.oid, pgtype.BinaryFormatCode, binary)
				require.NoError(t, err)
				requireUTCTimestamps(t, value, timestamp.want)
			})
		}

		// Leave multirange arrays unregistered so PeerDB keeps PostgreSQL's original text formatting.
		_, registered := typeMap.TypeForOID(timestamp.multirangeArrayOID)
		require.False(t, registered)
	}

	// Configuring one map must not mutate pgx's defaults for other connections.
	unconfigured := pgtype.NewMap()
	typ, ok := unconfigured.TypeForOID(pgtype.TimestamptzOID)
	require.True(t, ok)
	value, err := typ.Codec.DecodeValue(unconfigured, pgtype.TimestamptzOID,
		pgtype.TextFormatCode, []byte("2024-01-02 03:04:05+00"))
	require.NoError(t, err)
	require.Same(t, time.Local, value.(time.Time).Location())
}

func requireUTCTimestamps(t *testing.T, value any, want time.Time) {
	t.Helper()
	switch value := value.(type) {
	case time.Time:
		require.Same(t, time.UTC, value.Location())
		require.True(t, value.Equal(want), "got %s, want %s", value, want)
	case []any:
		require.NotEmpty(t, value)
		for _, element := range value {
			requireUTCTimestamps(t, element, want)
		}
	case pgtype.Range[any]:
		require.True(t, value.Valid)
		requireUTCTimestamps(t, value.Lower, want)
		requireUTCTimestamps(t, value.Upper, want)
	case pgtype.Multirange[pgtype.Range[any]]:
		require.NotEmpty(t, value)
		for _, element := range value {
			requireUTCTimestamps(t, element, want)
		}
	default:
		t.Fatalf("unexpected decoded timestamp type %T", value)
	}
}
