package connpostgres

import (
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

var AddAllColumnTypesFields = []*protos.FieldDescription{
	{
		Name:         "id",
		Type:         qvalue.QType{Kind: qvalue.QKindInt32}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "c1",
		Type:         qvalue.QType{Kind: qvalue.QKindBit}.String(),
		TypeModifier: 1,
	},
	{
		Name:         "c2",
		Type:         qvalue.QType{Kind: qvalue.QKindBoolean}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "c3",
		Type:         qvalue.QType{Kind: qvalue.QKindBytes}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "c4",
		Type:         qvalue.QType{Kind: qvalue.QKindDate}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "c5",
		Type:         qvalue.QType{Kind: qvalue.QKindFloat32}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "c6",
		Type:         qvalue.QType{Kind: qvalue.QKindFloat64}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "c7",
		Type:         qvalue.QType{Kind: qvalue.QKindInt16}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "c8",
		Type:         qvalue.QType{Kind: qvalue.QKindInt32}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "c9",
		Type:         qvalue.QType{Kind: qvalue.QKindInt64}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "c10",
		Type:         qvalue.QType{Kind: qvalue.QKindJSON}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "c11",
		Type:         qvalue.QType{Kind: qvalue.QKindNumeric}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "c12",
		Type:         qvalue.QType{Kind: qvalue.QKindString}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "c13",
		Type:         qvalue.QType{Kind: qvalue.QKindQChar}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "c14",
		Type:         qvalue.QType{Kind: qvalue.QKindTime}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "c15",
		Type:         qvalue.QType{Kind: qvalue.QKindTimestamp}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "c16",
		Type:         qvalue.QType{Kind: qvalue.QKindTimestampTZ}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "c17",
		Type:         qvalue.QType{Kind: qvalue.QKindUUID}.String(),
		TypeModifier: -1,
	},
}

var TrickyFields = []*protos.FieldDescription{
	{
		Name:         "id",
		Type:         qvalue.QType{Kind: qvalue.QKindInt32}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "c1",
		Type:         qvalue.QType{Kind: qvalue.QKindString}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "C1",
		Type:         qvalue.QType{Kind: qvalue.QKindString}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "C 1",
		Type:         qvalue.QType{Kind: qvalue.QKindString}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "right",
		Type:         qvalue.QType{Kind: qvalue.QKindString}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "select",
		Type:         qvalue.QType{Kind: qvalue.QKindString}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "XMIN",
		Type:         qvalue.QType{Kind: qvalue.QKindString}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "Cariño",
		Type:         qvalue.QType{Kind: qvalue.QKindString}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "±ªþ³§",
		Type:         qvalue.QType{Kind: qvalue.QKindString}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "カラム",
		Type:         qvalue.QType{Kind: qvalue.QKindString}.String(),
		TypeModifier: -1,
	},
}

var WhitespaceFields = []*protos.FieldDescription{
	{
		Name:         " ",
		Type:         qvalue.QType{Kind: qvalue.QKindInt32}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "  ",
		Type:         qvalue.QType{Kind: qvalue.QKindString}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "   ",
		Type:         qvalue.QType{Kind: qvalue.QKindInt64}.String(),
		TypeModifier: -1,
	},
	{
		Name:         "\t",
		Type:         qvalue.QType{Kind: qvalue.QKindDate}.String(),
		TypeModifier: -1,
	},
}
