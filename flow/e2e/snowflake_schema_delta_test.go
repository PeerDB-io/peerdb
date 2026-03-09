package e2e

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	connsnowflake "github.com/PeerDB-io/peerdb/flow/connectors/snowflake"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

const (
	schemaDeltaTestSchemaName            = "PUBLIC"
	numericAddedColumnTypeModifier int32 = 1048587 // Numeric(16,7)
)

type SnowflakeSchemaDeltaTestSuite struct {
	t *testing.T

	connector    *connsnowflake.SnowflakeConnector
	sfTestHelper *SnowflakeTestHelper
}

func setupSchemaDeltaSuite(t *testing.T) SnowflakeSchemaDeltaTestSuite {
	t.Helper()

	sfTestHelper, err := NewSnowflakeTestHelper(t)
	if err != nil {
		t.Fatalf("Error in test: %v", err)
	}

	connector, err := connsnowflake.NewSnowflakeConnector(
		t.Context(),
		sfTestHelper.Config,
	)
	if err != nil {
		t.Fatalf("Error in test: %v", err)
	}

	return SnowflakeSchemaDeltaTestSuite{
		t:            t,
		connector:    connector,
		sfTestHelper: sfTestHelper,
	}
}

func (s SnowflakeSchemaDeltaTestSuite) TestSimpleAddColumn() {
	tableName := schemaDeltaTestSchemaName + ".SIMPLE_ADD_COLUMN"
	err := s.sfTestHelper.RunCommand(s.t.Context(), fmt.Sprintf("CREATE TABLE %s(ID TEXT PRIMARY KEY)", tableName))
	require.NoError(s.t, err)

	require.NoError(s.t, s.connector.ReplayTableSchemaDeltas(s.t.Context(), nil, "schema_delta_flow", nil, []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: []*protos.FieldDescription{
			{
				Name:         "HI",
				Type:         string(types.QValueKindJSON),
				TypeModifier: -1,
			},
		},
	}}, nil))

	output, err := s.connector.GetTableSchema(s.t.Context(), nil, 0, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: tableName}})
	require.NoError(s.t, err)
	require.Equal(s.t, &protos.TableSchema{
		TableIdentifier: tableName,
		Columns: []*protos.FieldDescription{
			{
				Name:         "ID",
				Type:         string(types.QValueKindString),
				TypeModifier: -1,
			},
			{
				Name:         "HI",
				Type:         string(types.QValueKindJSON),
				TypeModifier: -1,
			},
		},
	}, output[tableName])
}

func (s SnowflakeSchemaDeltaTestSuite) TestAddAllColumnTypes() {
	tableName := schemaDeltaTestSchemaName + ".ADD_DROP_ALL_COLUMN_TYPES"
	err := s.sfTestHelper.RunCommand(s.t.Context(), fmt.Sprintf("CREATE TABLE %s(ID TEXT PRIMARY KEY)", tableName))
	require.NoError(s.t, err)

	expectedTableSchema := &protos.TableSchema{
		TableIdentifier: tableName,
		Columns: []*protos.FieldDescription{
			{
				Name:         "ID",
				Type:         string(types.QValueKindString),
				TypeModifier: -1,
			},
			{
				Name:         "C1",
				Type:         string(types.QValueKindBoolean),
				TypeModifier: -1,
			},
			{
				Name:         "C2",
				Type:         string(types.QValueKindBytes),
				TypeModifier: -1,
			},
			{
				Name:         "C3",
				Type:         string(types.QValueKindDate),
				TypeModifier: -1,
			},
			{
				Name:         "C4",
				Type:         string(types.QValueKindFloat64),
				TypeModifier: -1,
			},
			{
				Name:         "C5",
				Type:         string(types.QValueKindJSON),
				TypeModifier: -1,
			},
			{
				Name:         "C6",
				Type:         string(types.QValueKindNumeric),
				TypeModifier: numericAddedColumnTypeModifier, // Numeric(16,7)
			},
			{
				Name:         "C7",
				Type:         string(types.QValueKindString),
				TypeModifier: -1,
			},
			{
				Name:         "C8",
				Type:         string(types.QValueKindTime),
				TypeModifier: -1,
			},
			{
				Name:         "C9",
				Type:         string(types.QValueKindTimestamp),
				TypeModifier: -1,
			},
			{
				Name:         "C10",
				Type:         string(types.QValueKindTimestampTZ),
				TypeModifier: -1,
			},
		},
	}
	addedColumns := make([]*protos.FieldDescription, 0)
	for _, column := range expectedTableSchema.Columns {
		if column.Name != "ID" {
			var typeModifierOfAddedCol int32
			typeModifierOfAddedCol = -1
			if column.Type == string(types.QValueKindNumeric) {
				typeModifierOfAddedCol = numericAddedColumnTypeModifier
			}
			addedColumns = append(addedColumns, &protos.FieldDescription{
				Name:         column.Name,
				Type:         column.Type,
				TypeModifier: typeModifierOfAddedCol,
			},
			)
		}
	}

	require.NoError(s.t, s.connector.ReplayTableSchemaDeltas(s.t.Context(), nil, "schema_delta_flow", nil, []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: addedColumns,
	}}, nil))

	output, err := s.connector.GetTableSchema(s.t.Context(), nil, 0, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: tableName}})
	require.NoError(s.t, err)
	require.Equal(s.t, expectedTableSchema, output[tableName])
}

func (s SnowflakeSchemaDeltaTestSuite) TestAddTrickyColumnNames() {
	tableName := schemaDeltaTestSchemaName + ".ADD_DROP_TRICKY_COLUMN_NAMES"
	err := s.sfTestHelper.RunCommand(s.t.Context(), fmt.Sprintf("CREATE TABLE %s(id TEXT PRIMARY KEY)", tableName))
	require.NoError(s.t, err)

	expectedTableSchema := &protos.TableSchema{
		TableIdentifier: tableName,
		Columns: []*protos.FieldDescription{
			{
				Name:         "ID",
				Type:         string(types.QValueKindString),
				TypeModifier: -1,
			},
			{
				Name:         "C1",
				Type:         string(types.QValueKindString),
				TypeModifier: -1,
			},
			{
				Name:         "C 1",
				Type:         string(types.QValueKindString),
				TypeModifier: -1,
			},
			{
				Name:         "RIGHT",
				Type:         string(types.QValueKindString),
				TypeModifier: -1,
			},
			{
				Name:         "SELECT",
				Type:         string(types.QValueKindString),
				TypeModifier: -1,
			},
			{
				Name:         "XMIN",
				Type:         string(types.QValueKindString),
				TypeModifier: -1,
			},
			{
				Name:         "CARIÑO",
				Type:         string(types.QValueKindString),
				TypeModifier: -1,
			},
			{
				Name:         "±ªÞ³§",
				Type:         string(types.QValueKindString),
				TypeModifier: -1,
			},
			{
				Name:         "カラム",
				Type:         string(types.QValueKindString),
				TypeModifier: -1,
			},
		},
	}
	addedColumns := make([]*protos.FieldDescription, 0)
	for _, column := range expectedTableSchema.Columns {
		if column.Name != "ID" {
			addedColumns = append(addedColumns, &protos.FieldDescription{
				Name:         column.Name,
				Type:         column.Type,
				TypeModifier: -1,
			},
			)
		}
	}

	require.NoError(s.t, s.connector.ReplayTableSchemaDeltas(s.t.Context(), nil, "schema_delta_flow", nil, []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: addedColumns,
	}}, nil))

	output, err := s.connector.GetTableSchema(s.t.Context(), nil, 0, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: tableName}})
	require.NoError(s.t, err)
	require.Equal(s.t, expectedTableSchema, output[tableName])
}

func (s SnowflakeSchemaDeltaTestSuite) TestAddWhitespaceColumnNames() {
	tableName := schemaDeltaTestSchemaName + ".ADD_DROP_WHITESPACE_COLUMN_NAMES"
	err := s.sfTestHelper.RunCommand(s.t.Context(), fmt.Sprintf("CREATE TABLE %s(\" \" TEXT PRIMARY KEY)", tableName))
	require.NoError(s.t, err)

	expectedTableSchema := &protos.TableSchema{
		TableIdentifier: tableName,
		Columns: []*protos.FieldDescription{
			{
				Name:         " ",
				Type:         string(types.QValueKindString),
				TypeModifier: -1,
			},
			{
				Name:         "  ",
				Type:         string(types.QValueKindString),
				TypeModifier: -1,
			},
			{
				Name:         "   ",
				Type:         string(types.QValueKindTime),
				TypeModifier: -1,
			},
			{
				Name:         "\t",
				Type:         string(types.QValueKindDate),
				TypeModifier: -1,
			},
		},
	}

	addedColumns := make([]*protos.FieldDescription, 0)
	for _, column := range expectedTableSchema.Columns {
		if column.Name != " " {
			addedColumns = append(addedColumns, &protos.FieldDescription{
				Name:         column.Name,
				Type:         column.Type,
				TypeModifier: -1,
			},
			)
		}
	}

	require.NoError(s.t, s.connector.ReplayTableSchemaDeltas(s.t.Context(), nil, "schema_delta_flow", nil, []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: addedColumns,
	}}, nil))

	output, err := s.connector.GetTableSchema(s.t.Context(), nil, 0, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: tableName}})
	require.NoError(s.t, err)
	require.Equal(s.t, expectedTableSchema, output[tableName])
}

func (s SnowflakeSchemaDeltaTestSuite) Teardown(ctx context.Context) {
	require.NoError(s.t, s.sfTestHelper.Cleanup(ctx))
	require.NoError(s.t, s.connector.Close())
}

func TestSnowflakeSchemaDeltaTestSuite(t *testing.T) {
	e2eshared.RunSuite(t, setupSchemaDeltaSuite)
}
