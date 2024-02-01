package connpostgres

import (
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

var AddAllColumnTypes = []string{
	string(qvalue.QValueKindInt32),
	string(qvalue.QValueKindBit),
	string(qvalue.QValueKindBoolean),
	string(qvalue.QValueKindBytes),
	string(qvalue.QValueKindDate),
	string(qvalue.QValueKindFloat32),
	string(qvalue.QValueKindFloat64),
	string(qvalue.QValueKindInt16),
	string(qvalue.QValueKindInt32),
	string(qvalue.QValueKindInt64),
	string(qvalue.QValueKindJSON),
	string(qvalue.QValueKindNumeric),
	string(qvalue.QValueKindString),
	string(qvalue.QValueKindTime),
	string(qvalue.QValueKindTimestamp),
	string(qvalue.QValueKindTimestampTZ),
	string(qvalue.QValueKindUUID),
}

var AddAllColumnTypesFields = []*protos.FieldDescription{
	{
		ColumnName:   "id",
		ColumnType:   string(qvalue.QValueKindInt32),
		TypeModifier: -1,
	},
	{
		ColumnName:   "c1",
		ColumnType:   string(qvalue.QValueKindBit),
		TypeModifier: 1,
	},
	{
		ColumnName:   "c2",
		ColumnType:   string(qvalue.QValueKindBoolean),
		TypeModifier: -1,
	},
	{
		ColumnName:   "c3",
		ColumnType:   string(qvalue.QValueKindBytes),
		TypeModifier: -1,
	},
	{
		ColumnName:   "c4",
		ColumnType:   string(qvalue.QValueKindDate),
		TypeModifier: -1,
	},
	{
		ColumnName:   "c5",
		ColumnType:   string(qvalue.QValueKindFloat32),
		TypeModifier: -1,
	},
	{
		ColumnName:   "c6",
		ColumnType:   string(qvalue.QValueKindFloat64),
		TypeModifier: -1,
	},
	{
		ColumnName:   "c7",
		ColumnType:   string(qvalue.QValueKindInt16),
		TypeModifier: -1,
	},
	{
		ColumnName:   "c8",
		ColumnType:   string(qvalue.QValueKindInt32),
		TypeModifier: -1,
	},
	{
		ColumnName:   "c9",
		ColumnType:   string(qvalue.QValueKindInt64),
		TypeModifier: -1,
	},
	{
		ColumnName:   "c10",
		ColumnType:   string(qvalue.QValueKindJSON),
		TypeModifier: -1,
	},
	{
		ColumnName:   "c11",
		ColumnType:   string(qvalue.QValueKindNumeric),
		TypeModifier: -1,
	},
	{
		ColumnName:   "c12",
		ColumnType:   string(qvalue.QValueKindString),
		TypeModifier: -1,
	},
	{
		ColumnName:   "c13",
		ColumnType:   string(qvalue.QValueKindTime),
		TypeModifier: -1,
	},
	{
		ColumnName:   "c14",
		ColumnType:   string(qvalue.QValueKindTimestamp),
		TypeModifier: -1,
	},
	{
		ColumnName:   "c15",
		ColumnType:   string(qvalue.QValueKindTimestampTZ),
		TypeModifier: -1,
	},
	{
		ColumnName:   "c16",
		ColumnType:   string(qvalue.QValueKindUUID),
		TypeModifier: -1,
	},
}

var TrickyColumnTypes = []string{
	string(qvalue.QValueKindInt32),
	string(qvalue.QValueKindString),
	string(qvalue.QValueKindString),
	string(qvalue.QValueKindString),
	string(qvalue.QValueKindString),
	string(qvalue.QValueKindString),
	string(qvalue.QValueKindString),
	string(qvalue.QValueKindString),
	string(qvalue.QValueKindString),
	string(qvalue.QValueKindString),
}

var TrickyFields = []*protos.FieldDescription{
	{
		ColumnName:   "id",
		ColumnType:   string(qvalue.QValueKindInt32),
		TypeModifier: -1,
	},
	{
		ColumnName:   "c1",
		ColumnType:   string(qvalue.QValueKindString),
		TypeModifier: -1,
	},
	{
		ColumnName:   "C1",
		ColumnType:   string(qvalue.QValueKindString),
		TypeModifier: -1,
	},
	{
		ColumnName:   "C 1",
		ColumnType:   string(qvalue.QValueKindString),
		TypeModifier: -1,
	},
	{
		ColumnName:   "right",
		ColumnType:   string(qvalue.QValueKindString),
		TypeModifier: -1,
	},
	{
		ColumnName:   "select",
		ColumnType:   string(qvalue.QValueKindString),
		TypeModifier: -1,
	},
	{
		ColumnName:   "XMIN",
		ColumnType:   string(qvalue.QValueKindString),
		TypeModifier: -1,
	},
	{
		ColumnName:   "Cariño",
		ColumnType:   string(qvalue.QValueKindString),
		TypeModifier: -1,
	},
	{
		ColumnName:   "±ªþ³§",
		ColumnType:   string(qvalue.QValueKindString),
		TypeModifier: -1,
	},
	{
		ColumnName:   "カラム",
		ColumnType:   string(qvalue.QValueKindString),
		TypeModifier: -1,
	},
}

var WhitespaceFields = []*protos.FieldDescription{
	{
		ColumnName:   " ",
		ColumnType:   string(qvalue.QValueKindInt32),
		TypeModifier: -1,
	},
	{
		ColumnName:   "  ",
		ColumnType:   string(qvalue.QValueKindString),
		TypeModifier: -1,
	},
	{
		ColumnName:   "   ",
		ColumnType:   string(qvalue.QValueKindInt64),
		TypeModifier: -1,
	},
	{
		ColumnName:   "\t",
		ColumnType:   string(qvalue.QValueKindDate),
		TypeModifier: -1,
	},
}
