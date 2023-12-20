package qvalue

type QDWHType int

const (
	QDWHTypeSnowflake  QDWHType = 2
	QDWHTypeBigQuery   QDWHType = 3
	QDWHTypeClickhouse QDWHType = 4
)
