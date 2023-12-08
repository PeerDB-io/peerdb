package e2e_snowflake

import (
	"context"
	"fmt"
	"testing"

	connsnowflake "github.com/PeerDB-io/peer-flow/connectors/snowflake"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/sirupsen/logrus"
	"github.com/ysmood/got"
)

const schemaDeltaTestSchemaName = "PUBLIC"

type SnowflakeSchemaDeltaTestSuite struct {
	got.G
	t *testing.T

	connector    *connsnowflake.SnowflakeConnector
	sfTestHelper *SnowflakeTestHelper
}

func (suite SnowflakeSchemaDeltaTestSuite) failTestError(err error) {
	if err != nil {
		logrus.Errorf("Error in test: %v", err)
		suite.FailNow()
	}
}

func setupSchemaDeltaSuite(
	t *testing.T,
	g got.G,
) SnowflakeSchemaDeltaTestSuite {
	sfTestHelper, err := NewSnowflakeTestHelper()
	if err != nil {
		logrus.Errorf("Error in test: %v", err)
		t.FailNow()
	}

	connector, err := connsnowflake.NewSnowflakeConnector(
		context.Background(),
		sfTestHelper.Config,
	)
	if err != nil {
		logrus.Errorf("Error in test: %v", err)
		t.FailNow()
	}

	return SnowflakeSchemaDeltaTestSuite{
		G:            g,
		t:            t,
		connector:    connector,
		sfTestHelper: sfTestHelper,
	}
}

func (suite SnowflakeSchemaDeltaTestSuite) tearDownSuite() {
	err := suite.sfTestHelper.Cleanup()
	suite.failTestError(err)
	err = suite.connector.Close()
	suite.failTestError(err)
}

func (suite SnowflakeSchemaDeltaTestSuite) TestSimpleAddColumn() {
	tableName := fmt.Sprintf("%s.SIMPLE_ADD_COLUMN", schemaDeltaTestSchemaName)
	err := suite.sfTestHelper.RunCommand(fmt.Sprintf("CREATE TABLE %s(ID TEXT PRIMARY KEY)", tableName))
	suite.failTestError(err)

	err = suite.connector.ReplayTableSchemaDeltas("schema_delta_flow", []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: []*protos.DeltaAddedColumn{{
			ColumnName: "HI",
			ColumnType: string(qvalue.QValueKindJSON),
		}},
	}})
	suite.failTestError(err)

	output, err := suite.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
		TableIdentifiers: []string{tableName},
	})
	suite.failTestError(err)
	suite.Equal(&protos.TableSchema{
		TableIdentifier: tableName,
		Columns: map[string]string{
			"ID": string(qvalue.QValueKindString),
			"HI": string(qvalue.QValueKindJSON),
		},
	}, output.TableNameSchemaMapping[tableName])
}

func (suite SnowflakeSchemaDeltaTestSuite) TestAddAllColumnTypes() {
	tableName := fmt.Sprintf("%s.ADD_DROP_ALL_COLUMN_TYPES", schemaDeltaTestSchemaName)
	err := suite.sfTestHelper.RunCommand(fmt.Sprintf("CREATE TABLE %s(ID TEXT PRIMARY KEY)", tableName))
	suite.failTestError(err)

	expectedTableSchema := &protos.TableSchema{
		TableIdentifier: tableName,
		// goal is to test all types we're currently mapping to, not all QValue types
		Columns: map[string]string{
			"ID":  string(qvalue.QValueKindString),
			"C1":  string(qvalue.QValueKindBoolean),
			"C2":  string(qvalue.QValueKindBytes),
			"C3":  string(qvalue.QValueKindDate),
			"C4":  string(qvalue.QValueKindFloat64),
			"C5":  string(qvalue.QValueKindJSON),
			"C6":  string(qvalue.QValueKindNumeric),
			"C7":  string(qvalue.QValueKindString),
			"C8":  string(qvalue.QValueKindTime),
			"C9":  string(qvalue.QValueKindTimestamp),
			"C10": string(qvalue.QValueKindTimestampTZ),
		},
	}
	addedColumns := make([]*protos.DeltaAddedColumn, 0)
	for columnName, columnType := range expectedTableSchema.Columns {
		if columnName != "ID" {
			addedColumns = append(addedColumns, &protos.DeltaAddedColumn{
				ColumnName: columnName,
				ColumnType: columnType,
			})
		}
	}

	err = suite.connector.ReplayTableSchemaDeltas("schema_delta_flow", []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: addedColumns,
	}})
	suite.failTestError(err)

	output, err := suite.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
		TableIdentifiers: []string{tableName},
	})
	suite.failTestError(err)
	suite.Equal(expectedTableSchema, output.TableNameSchemaMapping[tableName])
}

func (suite SnowflakeSchemaDeltaTestSuite) TestAddTrickyColumnNames() {
	tableName := fmt.Sprintf("%s.ADD_DROP_TRICKY_COLUMN_NAMES", schemaDeltaTestSchemaName)
	err := suite.sfTestHelper.RunCommand(fmt.Sprintf("CREATE TABLE %s(id TEXT PRIMARY KEY)", tableName))
	suite.failTestError(err)

	expectedTableSchema := &protos.TableSchema{
		TableIdentifier: tableName,
		// strings.ToUpper also does Unicode uppercasing :)
		Columns: map[string]string{
			"ID":     string(qvalue.QValueKindString),
			"C1":     string(qvalue.QValueKindString),
			"C 1":    string(qvalue.QValueKindString),
			"RIGHT":  string(qvalue.QValueKindString),
			"SELECT": string(qvalue.QValueKindString),
			"XMIN":   string(qvalue.QValueKindString),
			"CARIÑO": string(qvalue.QValueKindString),
			"±ªÞ³§":  string(qvalue.QValueKindString),
			"カラム":    string(qvalue.QValueKindString),
		},
	}
	addedColumns := make([]*protos.DeltaAddedColumn, 0)
	for columnName, columnType := range expectedTableSchema.Columns {
		if columnName != "ID" {
			addedColumns = append(addedColumns, &protos.DeltaAddedColumn{
				ColumnName: columnName,
				ColumnType: columnType,
			})
		}
	}

	err = suite.connector.ReplayTableSchemaDeltas("schema_delta_flow", []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: addedColumns,
	}})
	suite.failTestError(err)

	output, err := suite.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
		TableIdentifiers: []string{tableName},
	})
	suite.failTestError(err)
	suite.Equal(expectedTableSchema, output.TableNameSchemaMapping[tableName])
}

func (suite SnowflakeSchemaDeltaTestSuite) TestAddWhitespaceColumnNames() {
	tableName := fmt.Sprintf("%s.ADD_DROP_WHITESPACE_COLUMN_NAMES", schemaDeltaTestSchemaName)
	err := suite.sfTestHelper.RunCommand(fmt.Sprintf("CREATE TABLE %s(\" \" TEXT PRIMARY KEY)", tableName))
	suite.failTestError(err)

	expectedTableSchema := &protos.TableSchema{
		TableIdentifier: tableName,
		Columns: map[string]string{
			" ":   string(qvalue.QValueKindString),
			"  ":  string(qvalue.QValueKindString),
			"   ": string(qvalue.QValueKindTime),
			"	":   string(qvalue.QValueKindDate),
		},
	}
	addedColumns := make([]*protos.DeltaAddedColumn, 0)
	for columnName, columnType := range expectedTableSchema.Columns {
		if columnName != " " {
			addedColumns = append(addedColumns, &protos.DeltaAddedColumn{
				ColumnName: columnName,
				ColumnType: columnType,
			})
		}
	}

	err = suite.connector.ReplayTableSchemaDeltas("schema_delta_flow", []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: addedColumns,
	}})
	suite.failTestError(err)

	output, err := suite.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
		TableIdentifiers: []string{tableName},
	})
	suite.failTestError(err)
	suite.Equal(expectedTableSchema, output.TableNameSchemaMapping[tableName])
}

func TestSnowflakeSchemaDeltaTestSuite(t *testing.T) {
	got.Each(t, func(t *testing.T) SnowflakeSchemaDeltaTestSuite {
		g := got.New(t)

		g.Parallel()

		suite := setupSchemaDeltaSuite(t, g)

		g.Cleanup(func() {
			suite.tearDownSuite()
		})

		return suite
	})
}
