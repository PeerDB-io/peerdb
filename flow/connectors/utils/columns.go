package utils

import (
	"slices"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"golang.org/x/exp/maps"
)

func TableSchemaColumns(schema *protos.TableSchema) int {
	if schema.Columns != nil {
		return len(schema.Columns)
	} else {
		return len(schema.ColumnNames)
	}
}

func TableSchemaColumnNames(schema *protos.TableSchema) []string {
	if schema.Columns != nil {
		return maps.Keys(schema.Columns)
	} else {
		return slices.Clone(schema.ColumnNames)
	}
}

func IterColumns(schema *protos.TableSchema, iter func(k, v string)) {
	if schema.Columns != nil {
		for k, v := range schema.Columns {
			iter(k, v)
		}
	} else {
		for i, name := range schema.ColumnNames {
			iter(name, schema.ColumnTypes[i])
		}
	}
}

func IterColumnsError(schema *protos.TableSchema, iter func(k, v string) error) error {
	if schema.Columns != nil {
		for k, v := range schema.Columns {
			err := iter(k, v)
			if err != nil {
				return err
			}
		}
	} else {
		for i, name := range schema.ColumnNames {
			err := iter(name, schema.ColumnTypes[i])
			if err != nil {
				return err
			}
		}
	}
	return nil
}
