//nolint:all
package geo

import (
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"

	geom "github.com/twpayne/go-geos"
)

// returns the WKT representation of the geometry object if it is valid
func GeoValidate(hexWkb string) (string, error) {
	// Decode the WKB hex string into binary
	wkb, hexErr := hex.DecodeString(hexWkb)
	if hexErr != nil {
		slog.Warn(fmt.Sprintf("Ignoring invalid WKB: %s", hexWkb))
		return "", hexErr
	}

	// UnmarshalWKB performs geometry validation along with WKB parsing
	geometryObject, geoErr := geom.NewGeomFromWKB(wkb)
	if geoErr != nil {
		return "", geoErr
	}

	invalidReason := geometryObject.IsValidReason()
	if invalidReason != "Valid Geometry" {
		slog.Warn(fmt.Sprintf("Ignoring invalid geometry shape %s: %s", hexWkb, invalidReason))
		return "", errors.New(invalidReason)
	}

	wkt := geometryObject.ToWKT()
	return wkt, nil
}

// compares WKTs
func GeoCompare(wkt1, wkt2 string) bool {
	geom1, geoErr := geom.NewGeomFromWKT(wkt1)
	if geoErr != nil {
		return false
	}

	geom2, geoErr := geom.NewGeomFromWKT(wkt2)
	if geoErr != nil {
		return false
	}

	return geom1.Equals(geom2)
}
