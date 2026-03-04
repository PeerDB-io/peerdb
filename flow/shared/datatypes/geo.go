package datatypes

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/ewkb"
	"github.com/paulmach/orb/encoding/wkb"
	"github.com/paulmach/orb/encoding/wkt"
)

// GeoValidate returns the WKT representation of the geometry object if it is valid.
func GeoValidate(hexWkb string) (string, error) {
	// Decode the WKB hex string into binary
	wkbBytes, hexErr := hex.DecodeString(hexWkb)
	if hexErr != nil {
		return "", fmt.Errorf("ignoring invalid WKB: %s %w", wkbBytes, hexErr)
	}

	geom, srid, err := ewkb.Unmarshal(wkbBytes)
	if err != nil {
		return "", err
	}

	wktStr := GeosMarshalWKT(geom)

	if srid != 0 {
		wktStr = fmt.Sprintf("SRID=%d;%s", srid, wktStr)
	}
	return wktStr, nil
}

func GeoToWKB(wktStr string) ([]byte, error) {
	geom, err := wkt.Unmarshal(wktStr)
	if err != nil {
		return []byte{}, err
	}

	return wkb.Marshal(geom)
}

// GeosMarshalWKT marshals a geometry to WKT matching GEOS output format:
// space before opening paren, space after comma, decimal (not scientific) notation.
func GeosMarshalWKT(g orb.Geometry) string {
	var buf strings.Builder
	writeGeometry(&buf, g)
	return buf.String()
}

func writeCoord(buf *strings.Builder, p orb.Point) {
	buf.WriteString(strconv.FormatFloat(p[0], 'f', -1, 64))
	buf.WriteByte(' ')
	buf.WriteString(strconv.FormatFloat(p[1], 'f', -1, 64))
}

func writeCoordList(buf *strings.Builder, ps []orb.Point) {
	buf.WriteByte('(')
	for i, p := range ps {
		if i > 0 {
			buf.WriteString(", ")
		}
		writeCoord(buf, p)
	}
	buf.WriteByte(')')
}

func writeGeometry(buf *strings.Builder, g orb.Geometry) {
	switch g := g.(type) {
	case orb.Point:
		buf.WriteString("POINT (")
		writeCoord(buf, g)
		buf.WriteByte(')')
	case orb.MultiPoint:
		if len(g) == 0 {
			buf.WriteString("MULTIPOINT EMPTY")
			return
		}
		buf.WriteString("MULTIPOINT (")
		for i, p := range g {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteByte('(')
			writeCoord(buf, orb.Point(p))
			buf.WriteByte(')')
		}
		buf.WriteByte(')')
	case orb.LineString:
		if len(g) == 0 {
			buf.WriteString("LINESTRING EMPTY")
			return
		}
		buf.WriteString("LINESTRING ")
		writeCoordList(buf, g)
	case orb.MultiLineString:
		if len(g) == 0 {
			buf.WriteString("MULTILINESTRING EMPTY")
			return
		}
		buf.WriteString("MULTILINESTRING (")
		for i, ls := range g {
			if i > 0 {
				buf.WriteString(", ")
			}
			writeCoordList(buf, ls)
		}
		buf.WriteByte(')')
	case orb.Ring:
		writeGeometry(buf, orb.Polygon{g})
	case orb.Polygon:
		if len(g) == 0 {
			buf.WriteString("POLYGON EMPTY")
			return
		}
		buf.WriteString("POLYGON (")
		for i, r := range g {
			if i > 0 {
				buf.WriteString(", ")
			}
			writeCoordList(buf, orb.LineString(r))
		}
		buf.WriteByte(')')
	case orb.MultiPolygon:
		if len(g) == 0 {
			buf.WriteString("MULTIPOLYGON EMPTY")
			return
		}
		buf.WriteString("MULTIPOLYGON (")
		for i, p := range g {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteByte('(')
			for j, r := range p {
				if j > 0 {
					buf.WriteString(", ")
				}
				writeCoordList(buf, orb.LineString(r))
			}
			buf.WriteByte(')')
		}
		buf.WriteByte(')')
	case orb.Collection:
		if len(g) == 0 {
			buf.WriteString("GEOMETRYCOLLECTION EMPTY")
			return
		}
		buf.WriteString("GEOMETRYCOLLECTION (")
		for i, c := range g {
			if i > 0 {
				buf.WriteString(", ")
			}
			writeGeometry(buf, c)
		}
		buf.WriteByte(')')
	case orb.Bound:
		writeGeometry(buf, g.ToPolygon())
	}
}
