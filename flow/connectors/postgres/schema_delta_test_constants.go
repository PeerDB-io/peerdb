package connpostgres

import (
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

var AddAllColumnTypesFields = []*protos.FieldDescription{
	{
		Name:         "id",
		Type:         string(types.QValueKindInt32),
		TypeModifier: -1,
		Nullable:     false,
	},
	{
		Name:         "c2",
		Type:         string(types.QValueKindBoolean),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c3",
		Type:         string(types.QValueKindBytes),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c4",
		Type:         string(types.QValueKindDate),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c5",
		Type:         string(types.QValueKindFloat32),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c6",
		Type:         string(types.QValueKindFloat64),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c7",
		Type:         string(types.QValueKindInt16),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c8",
		Type:         string(types.QValueKindInt32),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c9",
		Type:         string(types.QValueKindInt64),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c10",
		Type:         string(types.QValueKindJSON),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c11",
		Type:         string(types.QValueKindNumeric),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c12",
		Type:         string(types.QValueKindString),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c13",
		Type:         string(types.QValueKindQChar),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c14",
		Type:         string(types.QValueKindTime),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c15",
		Type:         string(types.QValueKindTimestamp),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c16",
		Type:         string(types.QValueKindTimestampTZ),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "c17",
		Type:         string(types.QValueKindUUID),
		TypeModifier: -1,
		Nullable:     true,
	},
}

var TrickyFields = []*protos.FieldDescription{
	{
		Name:         "id",
		Type:         string(types.QValueKindInt32),
		TypeModifier: -1,
		Nullable:     false,
	},
	{
		Name:         "c1",
		Type:         string(types.QValueKindString),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "C1",
		Type:         string(types.QValueKindString),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "C 1",
		Type:         string(types.QValueKindString),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "right",
		Type:         string(types.QValueKindString),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "select",
		Type:         string(types.QValueKindString),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "XMIN",
		Type:         string(types.QValueKindString),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "Cariño",
		Type:         string(types.QValueKindString),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "±ªþ³§",
		Type:         string(types.QValueKindString),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "カラム",
		Type:         string(types.QValueKindString),
		TypeModifier: -1,
		Nullable:     true,
	},
}

var WhitespaceFields = []*protos.FieldDescription{
	{
		Name:         " ",
		Type:         string(types.QValueKindInt32),
		TypeModifier: -1,
		Nullable:     false,
	},
	{
		Name:         "  ",
		Type:         string(types.QValueKindString),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "   ",
		Type:         string(types.QValueKindInt64),
		TypeModifier: -1,
		Nullable:     true,
	},
	{
		Name:         "\t",
		Type:         string(types.QValueKindDate),
		TypeModifier: -1,
		Nullable:     true,
	},
}
