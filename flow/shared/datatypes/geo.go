package datatypes

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
		slog.Warn("Ignoring invalid WKB: " + hexWkb)
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
