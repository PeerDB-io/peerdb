package numeric

const (
	PeerDBNumericPrecision    = 38
	PeerDBNumericScale        = 20
	PeerDBClickhousePrecision = 76
	PeerDBClickhouseScale     = 38
)

// This is to reverse what make_numeric_typmod of Postgres does:
// https://github.com/postgres/postgres/blob/21912e3c0262e2cfe64856e028799d6927862563/src/backend/utils/adt/numeric.c#L897
func ParseNumericTypmod(typmod int32) (int16, int16) {
	offsetMod := typmod - 4
	precision := int16((offsetMod >> 16) & 0x7FFF)
	scale := int16(offsetMod & 0x7FFF)
	return precision, scale
}
