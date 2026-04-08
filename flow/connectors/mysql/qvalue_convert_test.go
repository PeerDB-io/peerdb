package connmysql

import (
	"encoding/binary"
	"math"
	"testing"
	"time"

	"github.com/twpayne/go-geom/encoding/wkb"
	"github.com/twpayne/go-geom/encoding/wkt"

	"github.com/stretchr/testify/require"
)

func TestProcessTime(t *testing.T) {
	epoch := time.Unix(0, 0).UTC()
	//nolint:govet
	for _, ts := range []struct {
		out time.Duration
		in  string
	}{
		{time.Date(1970, 1, 1, 23, 30, 0, 500000000, time.UTC).Sub(epoch), "23:30.5"},
		{time.Date(1970, 2, 3, 8, 0, 1, 0, time.UTC).Sub(epoch), "800:0:1"},
		{time.Date(1969, 11, 28, 15, 59, 59, 0, time.UTC).Sub(epoch), "-800:0:1"},
		{time.Date(1969, 11, 28, 15, 59, 58, 900000000, time.UTC).Sub(epoch), "-800:0:1.1"},
		{time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC).Sub(epoch), "1."},
		{time.Date(1970, 1, 1, 0, 12, 34, 0, time.UTC).Sub(epoch), "1234"},
		{time.Date(1970, 1, 1, 3, 12, 34, 0, time.UTC).Sub(epoch), "31234"},
		{time.Date(1970, 1, 1, 0, 0, 1, 120000000, time.UTC).Sub(epoch), "1.12"},
		{time.Date(1970, 1, 1, 0, 0, 1, 12000000, time.UTC).Sub(epoch), "1.012"},
		{time.Date(1970, 1, 1, 0, 0, 1, 12300000, time.UTC).Sub(epoch), "1.0123"},
		{time.Date(1970, 1, 1, 0, 0, 1, 1230000, time.UTC).Sub(epoch), "1.00123"},
		{time.Date(1970, 1, 1, 0, 0, 1, 1000, time.UTC).Sub(epoch), "1.000001"},
		{time.Date(1970, 1, 1, 0, 0, 1, 200, time.UTC).Sub(epoch), "1.0000002"},
		{time.Date(1970, 1, 1, 0, 0, 1, 30, time.UTC).Sub(epoch), "1.00000003"},
		{time.Date(1970, 1, 1, 0, 0, 1, 4, time.UTC).Sub(epoch), "1.000000004"},
		{0, "123.aa"},
		{0, "hh:00:00"},
		{0, "00:mm:00"},
		{0, "00:00:ss"},
		{0, "hh:00"},
		{0, "00:mm"},
		{0, "ss"},
		{0, "mm00"},
		{0, "00ss"},
		{0, "hh0000"},
		{0, "00mm00"},
		{0, "0000ss"},
	} {
		tm, err := processTime(ts.in)
		if tm == 0 {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, ts.out, tm)
	}
}

// wktToMySQLGeomBytes converts WKT to MySQL's internal geometry format (4-byte SRID LE + WKB).
func wktToMySQLGeomBytes(t *testing.T, wktStr string, srid uint32) []byte {
	t.Helper()
	g, err := wkt.Unmarshal(wktStr)
	require.NoError(t, err)
	wkbBytes, err := wkb.Marshal(g, wkb.NDR)
	require.NoError(t, err)
	buf := make([]byte, 4, 4+len(wkbBytes))
	binary.LittleEndian.PutUint32(buf, srid)
	return append(buf, wkbBytes...)
}

func TestProcessGeometryData(t *testing.T) {
	for _, tc := range []struct {
		name string
		wkt  string
		srid uint32
	}{
		{"point", "POINT (1 2)", 0},
		{"linestring", "LINESTRING (1 2, 3 4)", 0},
		{"polygon", "POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))", 0},
		{"multipoint", "MULTIPOINT (1 2, 3 4)", 0},
		{"multilinestring", "MULTILINESTRING ((1 2, 3 4), (5 6, 7 8))", 0},
		{"multipolygon", "MULTIPOLYGON (((1 1, 3 1, 3 3, 1 3, 1 1)), ((4 4, 6 4, 6 6, 4 6, 4 4)))", 0},
		{"geometrycollection", "GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (1 2, 3 4))", 0},

		// SRID is stripped by processGeometryData; verify all types work with non-zero SRID
		{"point_srid", "POINT (5 6)", 3857},
		{"linestring_srid", "LINESTRING (5 6, 7 8)", 3857},
		{"polygon_srid", "POLYGON ((5 5, 7 5, 7 7, 5 7, 5 5))", 3857},
		{"multipoint_srid", "MULTIPOINT (5 6, 7 8)", 3857},
		{"multilinestring_srid", "MULTILINESTRING ((5 6, 7 8), (9 10, 11 12))", 3857},
		{"multipolygon_srid", "MULTIPOLYGON (((5 5, 7 5, 7 7, 5 7, 5 5)), ((8 8, 10 8, 10 10, 8 10, 8 8)))", 3857},
		{"geometrycollection_srid", "GEOMETRYCOLLECTION (POINT (5 6), LINESTRING (5 6, 7 8))", 3857},

		// Nested geometry collection
		{"nested_collection", "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (1 2)), POINT (3 4))", 0},

		// Polygon with hole
		{"polygon_with_hole", "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 8 2, 8 8, 2 8, 2 2))", 0},

		// Multipolygon with holes
		{"multipolygon_with_holes", "MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 8 2, 8 8, 2 8, 2 2)), ((20 20, 30 20, 30 30, 20 30, 20 20)))", 0},

		// Negative coordinates
		{"point_negative", "POINT (-73.9857 40.7484)", 0},
		{"linestring_negative", "LINESTRING (-122.4194 37.7749, -73.9857 40.7484)", 0},
		{"polygon_negative", "POLYGON ((-10 -10, 10 -10, 10 10, -10 10, -10 -10))", 0},
		{"point_all_negative", "POINT (-180 -90)", 0},

		// Floating point coordinates
		{"point_float", "POINT (1.23456789 9.87654321)", 0},
		{"linestring_float", "LINESTRING (0.1 0.2, 3.14159 2.71828)", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := wktToMySQLGeomBytes(t, tc.wkt, tc.srid)
			result, err := processGeometryData(data)
			require.NoError(t, err)
			// SRID is stripped by processGeometryData
			require.Equal(t, tc.wkt, result.Val)
		})
	}
}

// https://bugs.mysql.com/bug.php?id=77505: MySQL <= 5.7.8 auto-closed unclosed polygon rings.
// go-geom handles these; go-geos would reject with "Points of LinearRing do not form a closed linestring".
func TestProcessGeometryData_UnclosedRings(t *testing.T) {
	for _, tc := range []struct {
		name string
		wkb  []byte
		wkt  string
	}{
		{"unclosed_polygon", buildWKBPolygon([][][2]float64{
			{{0, 0}, {1, 0}, {0, 1}},
		}), "POLYGON ((0 0, 1 0, 0 1))"},
		{"unclosed_polygon_with_hole", buildWKBPolygon([][][2]float64{
			{{0, 0}, {10, 0}, {10, 10}, {0, 10}, {0, 0}},
			{{2, 2}, {8, 2}, {8, 8}, {2, 8}},
		}), "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 8 2, 8 8, 2 8))"},
		{"unclosed_multipolygon", buildWKBMultiPolygon([][][][2]float64{
			{{{0, 0}, {1, 0}, {1, 1}, {0, 1}}},
			{{{2, 2}, {3, 2}, {3, 3}, {2, 3}, {2, 2}}},
		}), "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1)), ((2 2, 3 2, 3 3, 2 3, 2 2)))"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := processGeometryData(prependSRID(tc.wkb, 0))
			require.NoError(t, err)
			require.Equal(t, tc.wkt, result.Val)
		})
	}
}

func TestProcessGeometryData_Errors(t *testing.T) {
	t.Run("too_short", func(t *testing.T) {
		_, err := processGeometryData([]byte{0, 0, 0})
		require.ErrorContains(t, err, "geometry data too short")
	})

	t.Run("empty_after_srid", func(t *testing.T) {
		_, err := processGeometryData([]byte{0, 0, 0, 0})
		require.ErrorContains(t, err, "geometry data too short")
	})

	t.Run("invalid_wkb", func(t *testing.T) {
		_, err := processGeometryData([]byte{0, 0, 0, 0, 0xFF, 0xFF})
		require.Error(t, err)
		require.ErrorContains(t, err, "failed to parse geometry WKB")
	})
}

func buildWKBRing(points [][2]float64) []byte {
	var buf []byte
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(points)))
	for _, p := range points {
		buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(p[0]))
		buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(p[1]))
	}
	return buf
}

func buildWKBPolygon(rings [][][2]float64) []byte {
	buf := []byte{0x01}                            // little-endian
	buf = binary.LittleEndian.AppendUint32(buf, 3) // wkbPolygon
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(rings)))
	for _, ring := range rings {
		buf = append(buf, buildWKBRing(ring)...)
	}
	return buf
}

func buildWKBMultiPolygon(polygons [][][][2]float64) []byte {
	buf := []byte{0x01}                            // little-endian
	buf = binary.LittleEndian.AppendUint32(buf, 6) // wkbMultiPolygon
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(polygons)))
	for _, poly := range polygons {
		buf = append(buf, buildWKBPolygon(poly)...)
	}
	return buf
}

func prependSRID(wkbBytes []byte, srid uint32) []byte {
	buf := make([]byte, 4, 4+len(wkbBytes))
	binary.LittleEndian.PutUint32(buf, srid)
	return append(buf, wkbBytes...)
}
