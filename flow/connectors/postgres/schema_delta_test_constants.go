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
	string(qvalue.QValueKindQChar),
	string(qvalue.QValueKindTime),
	string(qvalue.QValueKindTimestamp),
	string(qvalue.QValueKindTimestampTZ),
	string(qvalue.QValueKindUUID),
}

var AddAllColumnTypesFields = []*protos.FieldDescription{
	{
		Name:         "id",
		Type:         string(qvalue.QValueKindInt32),
		TypeModifier: -1,
	},
	{
		Name:         "c1",
		Type:         string(qvalue.QValueKindBit),
		TypeModifier: 1,
	},
	{
		Name:         "c2",
		Type:         string(qvalue.QValueKindBoolean),
		TypeModifier: -1,
	},
	{
		Name:         "c3",
		Type:         string(qvalue.QValueKindBytes),
		TypeModifier: -1,
	},
	{
		Name:         "c4",
		Type:         string(qvalue.QValueKindDate),
		TypeModifier: -1,
	},
	{
		Name:         "c5",
		Type:         string(qvalue.QValueKindFloat32),
		TypeModifier: -1,
	},
	{
		Name:         "c6",
		Type:         string(qvalue.QValueKindFloat64),
		TypeModifier: -1,
	},
	{
		Name:         "c7",
		Type:         string(qvalue.QValueKindInt16),
		TypeModifier: -1,
	},
	{
		Name:         "c8",
		Type:         string(qvalue.QValueKindInt32),
		TypeModifier: -1,
	},
	{
		Name:         "c9",
		Type:         string(qvalue.QValueKindInt64),
		TypeModifier: -1,
	},
	{
		Name:         "c10",
		Type:         string(qvalue.QValueKindJSON),
		TypeModifier: -1,
	},
	{
		Name:         "c11",
		Type:         string(qvalue.QValueKindNumeric),
		TypeModifier: -1,
	},
	{
		Name:         "c12",
		Type:         string(qvalue.QValueKindString),
		TypeModifier: -1,
	},
	{
		Name:         "c13",
		Type:         string(qvalue.QValueKindQChar),
		TypeModifier: -1,
	},
	{
		Name:         "c14",
		Type:         string(qvalue.QValueKindTime),
		TypeModifier: -1,
	},
	{
		Name:         "c15",
		Type:         string(qvalue.QValueKindTimestamp),
		TypeModifier: -1,
	},
	{
		Name:         "c16",
		Type:         string(qvalue.QValueKindTimestampTZ),
		TypeModifier: -1,
	},
	{
		Name:         "c17",
		Type:         string(qvalue.QValueKindUUID),
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
		Name:         "id",
		Type:         string(qvalue.QValueKindInt32),
		TypeModifier: -1,
	},
	{
		Name:         "c1",
		Type:         string(qvalue.QValueKindString),
		TypeModifier: -1,
	},
	{
		Name:         "C1",
		Type:         string(qvalue.QValueKindString),
		TypeModifier: -1,
	},
	{
		Name:         "C 1",
		Type:         string(qvalue.QValueKindString),
		TypeModifier: -1,
	},
	{
		Name:         "right",
		Type:         string(qvalue.QValueKindString),
		TypeModifier: -1,
	},
	{
		Name:         "select",
		Type:         string(qvalue.QValueKindString),
		TypeModifier: -1,
	},
	{
		Name:         "XMIN",
		Type:         string(qvalue.QValueKindString),
		TypeModifier: -1,
	},
	{
		Name:         "Cariño",
		Type:         string(qvalue.QValueKindString),
		TypeModifier: -1,
	},
	{
		Name:         "±ªþ³§",
		Type:         string(qvalue.QValueKindString),
		TypeModifier: -1,
	},
	{
		Name:         "カラム",
		Type:         string(qvalue.QValueKindString),
		TypeModifier: -1,
	},
}

var WhitespaceFields = []*protos.FieldDescription{
	{
		Name:         " ",
		Type:         string(qvalue.QValueKindInt32),
		TypeModifier: -1,
	},
	{
		Name:         "  ",
		Type:         string(qvalue.QValueKindString),
		TypeModifier: -1,
	},
	{
		Name:         "   ",
		Type:         string(qvalue.QValueKindInt64),
		TypeModifier: -1,
	},
	{
		Name:         "\t",
		Type:         string(qvalue.QValueKindDate),
		TypeModifier: -1,
	},
}
