package utils

import (
	"slices"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

func TableSchemaColumns(schema *protos.TableSchema) int {
	return len(schema.ColumnNames)
}

func TableSchemaColumnNames(schema *protos.TableSchema) []string {
	return slices.Clone(schema.ColumnNames)
}

func IterColumns(schema *protos.TableSchema, iter func(k, v string)) {
	for i, name := range schema.ColumnNames {
		iter(name, schema.ColumnTypes[i])
	}
}

func IterColumnsError(schema *protos.TableSchema, iter func(k, v string) error) error {
	for i, name := range schema.ColumnNames {
		err := iter(name, schema.ColumnTypes[i])
		if err != nil {
			return err
		}
	}
	return nil
}
