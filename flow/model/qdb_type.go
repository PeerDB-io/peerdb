package model

type QDBType int

const (
	QDBTypeUnknown   QDBType = 0
	QDBTypePostgres  QDBType = 1
	QDBTypeSnowflake QDBType = 2
	QDBTypeBigQuery  QDBType = 3
)
