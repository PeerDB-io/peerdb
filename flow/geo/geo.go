package geo

import (
	"encoding/binary"

	"github.com/twpayne/go-geom/encoding/wkb"
	"github.com/twpayne/go-geom/encoding/wkt"
)

func WktToWkb(text string, endian binary.ByteOrder) ([]byte, error) {
	g, err := wkt.Unmarshal(text)
	if err != nil {
		return nil, err
	}

	return wkb.Marshal(g, endian)
}
