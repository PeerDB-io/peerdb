package datatypes

import (
	"encoding/hex"
	"fmt"

	geom "github.com/twpayne/go-geos"
)

// returns the WKT representation of the geometry object if it is valid
func GeoValidate(hexWkb string) (string, error) {
	// Decode the WKB hex string into binary
	wkb, hexErr := hex.DecodeString(hexWkb)
	if hexErr != nil {
		return "", fmt.Errorf("ignoring invalid WKB: %s %w", wkb, hexErr)
	}

	// UnmarshalWKB performs geometry validation along with WKB parsing
	geometryObject, geoErr := geom.NewGeomFromWKB(wkb)
	if geoErr != nil {
		return "", geoErr
	}

	invalidReason := geometryObject.IsValidReason()
	if invalidReason != "Valid Geometry" {
		return "", fmt.Errorf("ignoring invalid geometry shape %s: %s", hexWkb, invalidReason)
	}

	wkt := geometryObject.ToWKT()

	if SRID := geometryObject.SRID(); SRID != 0 {
		wkt = fmt.Sprintf("SRID=%d;%s", geometryObject.SRID(), wkt)
	}
	return wkt, nil
}

func GeoToWKB(wkt string) ([]byte, error) {
	// UnmarshalWKB performs geometry validation along with WKB parsing
	geometryObject, geoErr := geom.NewGeomFromWKT(wkt)
	if geoErr != nil {
		return []byte{}, geoErr
	}

	return geometryObject.ToWKB(), nil
}
