package connpostgres

import (
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

var AddAllColumnTypesFields = []*protos.FieldDescription{
	{
		Name:         "id",
		Type:         string(qvalue.QValueKindInt32),
		TypeModifier: -1,
		Nullable:     false,
	},
	{
		Name:         "c2",
		Type:         string(qvalue.QValueKindBoolean),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c3",
		Type:         string(qvalue.QValueKindBytes),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c4",
		Type:         string(qvalue.QValueKindDate),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c5",
		Type:         string(qvalue.QValueKindFloat32),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c6",
		Type:         string(qvalue.QValueKindFloat64),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c7",
		Type:         string(qvalue.QValueKindInt16),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c8",
		Type:         string(qvalue.QValueKindInt32),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c9",
		Type:         string(qvalue.QValueKindInt64),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c10",
		Type:         string(qvalue.QValueKindJSON),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c11",
		Type:         string(qvalue.QValueKindNumeric),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c12",
		Type:         string(qvalue.QValueKindString),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c13",
		Type:         string(qvalue.QValueKindQChar),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c14",
		Type:         string(qvalue.QValueKindTime),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c15",
		Type:         string(qvalue.QValueKindTimestamp),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c16",
		Type:         string(qvalue.QValueKindTimestampTZ),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c17",
		Type:         string(qvalue.QValueKindUUID),
		TypeModifier: -1,
		Nullable:     true,
	},
}

var TrickyFields = []*protos.FieldDescription{
	{
		Name:         "id",
		Type:         string(qvalue.QValueKindInt32),
		TypeModifier: -1,
		Nullable:     false,
	},
	{
		Name:         "c1",
		Type:         string(qvalue.QValueKindString),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "C1",
		Type:         string(qvalue.QValueKindString),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "C 1",
		Type:         string(qvalue.QValueKindString),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "right",
		Type:         string(qvalue.QValueKindString),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "select",
		Type:         string(qvalue.QValueKindString),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "XMIN",
		Type:         string(qvalue.QValueKindString),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "Cariño",
		Type:         string(qvalue.QValueKindString),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "±ªþ³§",
		Type:         string(qvalue.QValueKindString),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "カラム",
		Type:         string(qvalue.QValueKindString),
		TypeModifier: -1,
		Nullable:     true,
	},
}

var WhitespaceFields = []*protos.FieldDescription{
	{
		Name:         " ",
		Type:         string(qvalue.QValueKindInt32),
		TypeModifier: -1,
		Nullable:     false,
	},
	{
		Name:         "  ",
		Type:         string(qvalue.QValueKindString),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "   ",
		Type:         string(qvalue.QValueKindInt64),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "\t",
		Type:         string(qvalue.QValueKindDate),
		TypeModifier: -1,
		Nullable:     true,
	},
}
