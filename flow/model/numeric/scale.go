package numeric

const (
	PeerDBNumericPrecision    = 38
	PeerDBNumericScale        = 20
	PeerDBClickhousePrecision = 76
	PeerDBClickhouseScale     = 38

	VARHDRSZ = 4
)

func MakeNumericTypmod(precision int32, scale int32) int32 {
	if precision == 0 && scale == 0 {
		return -1
	}
	return (precision << 16) | (scale & 0x7ff) + VARHDRSZ
}

// This is to reverse what make_numeric_typmod of Postgres does:
// https://github.com/postgres/postgres/blob/21912e3c0262e2cfe64856e028799d6927862563/src/backend/utils/adt/numeric.c#L897
func ParseNumericTypmod(typmod int32) (int16, int16) {
	offsetMod := typmod - VARHDRSZ
	precision := int16((offsetMod >> 16) & 0x7FFF)
	scale := int16(offsetMod & 0x7FFF)
	return precision, scale
}
