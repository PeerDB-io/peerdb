package connsqlserver

import "github.com/PeerDB-io/peer-flow/model/qvalue"

var qValueKindToSQLServerTypeMap = map[qvalue.QValueKind]string{
	qvalue.QValueKindBoolean:     "BIT",
	qvalue.QValueKindInt16:       "SMALLINT",
	qvalue.QValueKindInt32:       "INT",
	qvalue.QValueKindInt64:       "BIGINT",
	qvalue.QValueKindFloat32:     "REAL",
	qvalue.QValueKindFloat64:     "FLOAT",
	qvalue.QValueKindNumeric:     "DECIMAL(38, 9)",
	qvalue.QValueKindString:      "NTEXT",
	qvalue.QValueKindJSON:        "NTEXT", // SQL Server doesn't have a native JSON type
	qvalue.QValueKindTimestamp:   "DATETIME2",
	qvalue.QValueKindTimestampTZ: "DATETIMEOFFSET",
	qvalue.QValueKindTime:        "TIME",
	qvalue.QValueKindDate:        "DATE",
	qvalue.QValueKindBit:         "BINARY",
	qvalue.QValueKindBytes:       "VARBINARY(MAX)",
	qvalue.QValueKindArray:       "NTEXT", // SQL Server doesn't support array type
	qvalue.QValueKindStruct:      "NTEXT", // SQL Server doesn't support struct type
	qvalue.QValueKindUUID:        "UNIQUEIDENTIFIER",
	qvalue.QValueKindTimeTZ:      "NTEXT", // SQL Server doesn't have a time with timezone type
	qvalue.QValueKindInvalid:     "NTEXT",
}

var sqlServerTypeToQValueKindMap = map[string]qvalue.QValueKind{
	"INT":              qvalue.QValueKindInt32,
	"BIGINT":           qvalue.QValueKindInt64,
	"REAL":             qvalue.QValueKindFloat32,
	"FLOAT":            qvalue.QValueKindFloat64,
	"NTEXT":            qvalue.QValueKindString,
	"TEXT":             qvalue.QValueKindString,
	"BIT":              qvalue.QValueKindBoolean,
	"DATETIME":         qvalue.QValueKindTimestamp,
	"DATETIME2":        qvalue.QValueKindTimestamp,
	"DATETIMEOFFSET":   qvalue.QValueKindTimestampTZ,
	"TIME":             qvalue.QValueKindTime,
	"DATE":             qvalue.QValueKindDate,
	"VARBINARY(MAX)":   qvalue.QValueKindBytes,
	"BINARY":           qvalue.QValueKindBit,
	"DECIMAL":          qvalue.QValueKindNumeric,
	"UNIQUEIDENTIFIER": qvalue.QValueKindUUID,
	"SMALLINT":         qvalue.QValueKindInt32,
	"TINYINT":          qvalue.QValueKindInt32,
	"CHAR":             qvalue.QValueKindString,
	"VARCHAR":          qvalue.QValueKindString,
	"NCHAR":            qvalue.QValueKindString,
	"NVARCHAR":         qvalue.QValueKindString,
}
