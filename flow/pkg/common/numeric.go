package common

// ParseNumericTypmod reverses what make_numeric_typmod of Postgres does:
// https://github.com/postgres/postgres/blob/21912e3c0262e2cfe64856e028799d6927862563/src/backend/utils/adt/numeric.c#L897
func ParseNumericTypmod(typmod int32) (int16, int16) {
	if typmod == -1 {
		return 0, 0
	}
	const varhdrsz = int32(4)
	offsetMod := typmod - varhdrsz
	precision := int16((offsetMod >> 16) & 0x7FFF)
	// scale is a signed 11-bit field, negative since Postgres 15 (numeric_typmod_scale)
	scale := int16(((offsetMod & 0x7FF) ^ 1024) - 1024)
	return precision, scale
}
