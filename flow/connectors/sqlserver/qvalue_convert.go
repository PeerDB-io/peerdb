package connsqlserver

import "github.com/PeerDB-io/peer-flow/model/qvalue"

var qValueKindToSQLServerTypeMap = map[qvalue.QKind]string{
	qvalue.QKindBoolean:     "BIT",
	qvalue.QKindInt16:       "SMALLINT",
	qvalue.QKindInt32:       "INT",
	qvalue.QKindInt64:       "BIGINT",
	qvalue.QKindFloat32:     "REAL",
	qvalue.QKindFloat64:     "FLOAT",
	qvalue.QKindNumeric:     "DECIMAL(38, 9)",
	qvalue.QKindQChar:       "CHAR",
	qvalue.QKindString:      "NTEXT",
	qvalue.QKindJSON:        "NTEXT", // SQL Server doesn't have a native JSON type
	qvalue.QKindTimestamp:   "DATETIME2",
	qvalue.QKindTimestampTZ: "DATETIMEOFFSET",
	qvalue.QKindTime:        "TIME",
	qvalue.QKindDate:        "DATE",
	qvalue.QKindBit:         "BINARY",
	qvalue.QKindBytes:       "VARBINARY(MAX)",
	qvalue.QKindStruct:      "NTEXT", // SQL Server doesn't support struct type
	qvalue.QKindUUID:        "UNIQUEIDENTIFIER",
	qvalue.QKindTimeTZ:      "NTEXT", // SQL Server doesn't have a time with timezone type
	qvalue.QKindInvalid:     "NTEXT",
	qvalue.QKindHStore:      "NTEXT", // SQL Server doesn't have a native HStore type

	// for all array types, we use NTEXT
	qvalue.QKindArray: "NTEXT",
}

var sqlServerTypeToQValueKindMap = map[string]qvalue.QKind{
	"INT":              qvalue.QKindInt32,
	"BIGINT":           qvalue.QKindInt64,
	"REAL":             qvalue.QKindFloat32,
	"FLOAT":            qvalue.QKindFloat64,
	"NTEXT":            qvalue.QKindString,
	"TEXT":             qvalue.QKindString,
	"BIT":              qvalue.QKindBoolean,
	"DATETIME":         qvalue.QKindTimestamp,
	"DATETIME2":        qvalue.QKindTimestamp,
	"DATETIMEOFFSET":   qvalue.QKindTimestampTZ,
	"TIME":             qvalue.QKindTime,
	"DATE":             qvalue.QKindDate,
	"VARBINARY(MAX)":   qvalue.QKindBytes,
	"BINARY":           qvalue.QKindBit,
	"DECIMAL":          qvalue.QKindNumeric,
	"UNIQUEIDENTIFIER": qvalue.QKindUUID,
	"SMALLINT":         qvalue.QKindInt32,
	"TINYINT":          qvalue.QKindInt32,
	"CHAR":             qvalue.QKindQChar,
	"VARCHAR":          qvalue.QKindString,
	"NCHAR":            qvalue.QKindString,
	"NVARCHAR":         qvalue.QKindString,
}
