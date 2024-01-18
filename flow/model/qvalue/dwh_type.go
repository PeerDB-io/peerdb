package qvalue

type QDWHType int

const (
	QDWHTypeS3         QDWHType = 1
	QDWHTypeSnowflake  QDWHType = 2
	QDWHTypeBigQuery   QDWHType = 3
	QDWHTypeClickhouse QDWHType = 4
)
