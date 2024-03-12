package qvalue

import "github.com/PeerDB-io/peer-flow/model/numeric"

type QDWHType int

const (
	QDWHTypeS3         QDWHType = 1
	QDWHTypeSnowflake  QDWHType = 2
	QDWHTypeBigQuery   QDWHType = 3
	QDWHTypeClickhouse QDWHType = 4
)

func DetermineNumericSettingForDWH(precision int16, scale int16, dwh QDWHType) (int16, int16) {
	if dwh == QDWHTypeClickhouse {
		if precision > numeric.PeerDBClickhousePrecision || precision <= 0 ||
			scale > precision || scale < 0 {
			return numeric.PeerDBClickhousePrecision, numeric.PeerDBClickhouseScale
		}
	} else {
		if precision > numeric.PeerDBNumericPrecision || precision <= 0 ||
			scale > numeric.PeerDBNumericScale || scale < 0 {
			return numeric.PeerDBNumericPrecision, numeric.PeerDBNumericScale
		}
	}

	return precision, scale
}
