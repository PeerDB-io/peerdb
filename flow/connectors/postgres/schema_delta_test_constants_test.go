package connpostgres

import (
	"fmt"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	numeric "github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

var qValueKindToPgName = map[string]string{
	string(types.QValueKindBoolean):     "bool",
	string(types.QValueKindBytes):       "bytea",
	string(types.QValueKindDate):        "date",
	string(types.QValueKindFloat32):     "float4",
	string(types.QValueKindFloat64):     "float8",
	string(types.QValueKindInt16):       "int2",
	string(types.QValueKindInt32):       "int4",
	string(types.QValueKindInt64):       "int8",
	string(types.QValueKindJSON):        "json",
	string(types.QValueKindNumeric):     "numeric",
	string(types.QValueKindString):      "text",
	string(types.QValueKindQChar):       "char",
	string(types.QValueKindTime):        "time",
	string(types.QValueKindTimestamp):   "timestamp",
	string(types.QValueKindTimestampTZ): "timestamptz",
	string(types.QValueKindUUID):        "uuid",
}

func fieldsForSystem(qFields []*protos.FieldDescription, system protos.TypeSystem) []*protos.FieldDescription {
	if system == protos.TypeSystem_Q {
		return qFields
	}
	result := make([]*protos.FieldDescription, len(qFields))
	for i, f := range qFields {
		pgType, ok := qValueKindToPgName[f.Type]
		if !ok {
			panic(fmt.Sprintf("no PG type mapping for QValueKind %q", f.Type))
		}
		result[i] = &protos.FieldDescription{
			Name:           f.Name,
			Type:           pgType,
			TypeModifier:   f.TypeModifier,
			Nullable:       f.Nullable,
			TypeSchemaName: f.TypeSchemaName,
		}
	}
	return result
}

var AddAllColumnTypesFields = []*protos.FieldDescription{
	{
		Name:           "id",
		Type:           string(types.QValueKindInt32),
		TypeModifier:   -1,
		Nullable:       false,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "c2",
		Type:           string(types.QValueKindBoolean),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "c3",
		Type:           string(types.QValueKindBytes),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "c4",
		Type:           string(types.QValueKindDate),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "c5",
		Type:           string(types.QValueKindFloat32),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "c6",
		Type:           string(types.QValueKindFloat64),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "c7",
		Type:           string(types.QValueKindInt16),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "c8",
		Type:           string(types.QValueKindInt32),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "c9",
		Type:           string(types.QValueKindInt64),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "c10",
		Type:           string(types.QValueKindJSON),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "c11",
		Type:           string(types.QValueKindNumeric),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "c12",
		Type:           string(types.QValueKindString),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "c13",
		Type:           string(types.QValueKindQChar),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "c14",
		Type:           string(types.QValueKindTime),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "c15",
		Type:           string(types.QValueKindTimestamp),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "c16",
		Type:           string(types.QValueKindTimestampTZ),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "c17",
		Type:           string(types.QValueKindUUID),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "c18",
		Type:           string(types.QValueKindNumeric),
		TypeModifier:   numeric.MakeNumericTypmod(10, 2),
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
}

var TrickyFields = []*protos.FieldDescription{
	{
		Name:           "id",
		Type:           string(types.QValueKindInt32),
		TypeModifier:   -1,
		Nullable:       false,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "c1",
		Type:           string(types.QValueKindString),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "C1",
		Type:           string(types.QValueKindString),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "C 1",
		Type:           string(types.QValueKindString),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "right",
		Type:           string(types.QValueKindString),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "select",
		Type:           string(types.QValueKindString),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "XMIN",
		Type:           string(types.QValueKindString),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "Cariño",
		Type:           string(types.QValueKindString),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "±ªþ³§",
		Type:           string(types.QValueKindString),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "カラム",
		Type:           string(types.QValueKindString),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
}

var WhitespaceFields = []*protos.FieldDescription{
	{
		Name:           " ",
		Type:           string(types.QValueKindInt32),
		TypeModifier:   -1,
		Nullable:       false,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "  ",
		Type:           string(types.QValueKindString),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "   ",
		Type:           string(types.QValueKindInt64),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
	{
		Name:           "\t",
		Type:           string(types.QValueKindDate),
		TypeModifier:   -1,
		Nullable:       true,
		TypeSchemaName: "pg_catalog",
	},
}
