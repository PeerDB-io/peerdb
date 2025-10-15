package connclickhouse

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	peerdb_clickhouse "github.com/PeerDB-io/peerdb/flow/pkg/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// insertFromTableFunctionConfig contains the configuration for building INSERT queries from table functions
type insertFromTableFunctionConfig struct {
	columnNameMap             map[string]string
	config                    *protos.QRepConfig
	connector                 *ClickHouseConnector
	logger                    log.Logger
	destinationTable          string
	schema                    types.QRecordSchema
	excludedColumns           []string
	fieldExpressionConverters []fieldExpressionConverter
}

type fieldExpressionConverter func(
	ctx context.Context,
	config *insertFromTableFunctionConfig,
	sourceFieldIdentifier string,
	field types.QField,
) (string, error)

func jsonFieldExpressionConverter(
	ctx context.Context,
	config *insertFromTableFunctionConfig,
	sourceFieldIdentifier string,
	field types.QField,
) (string, error) {
	if field.Type != types.QValueKindJSON && field.Type != types.QValueKindJSONB {
		return sourceFieldIdentifier, nil
	}

	if !qvalue.ShouldUseNativeJSONType(ctx, config.config.Env, config.connector.chVersion) {
		return sourceFieldIdentifier, nil
	}

	return fmt.Sprintf("CAST(%s, 'JSON')", sourceFieldIdentifier), nil
}

var defaultFieldExpressionConverters = []fieldExpressionConverter{
	jsonFieldExpressionConverter,
}

// buildInsertFromTableFunctionQuery builds a complete INSERT query from a table function expression
// This function handles column mapping, type conversions, and source schema columns
func buildInsertFromTableFunctionQuery(
	ctx context.Context,
	config *insertFromTableFunctionConfig,
	tableFunctionExpr string,
	settings map[string]string,
) (string, error) {
	fieldExpressionConverters := defaultFieldExpressionConverters
	fieldExpressionConverters = append(fieldExpressionConverters, config.fieldExpressionConverters...)

	sourceSchemaAsDestinationColumn, err := internal.PeerDBSourceSchemaAsDestinationColumn(ctx, config.config.Env)
	if err != nil {
		return "", err
	}

	selectedColumnNames := make([]string, 0, len(config.schema.Fields))
	insertedColumnNames := make([]string, 0, len(config.schema.Fields))

	for _, field := range config.schema.Fields {
		colName := field.Name

		// Skip excluded columns
		excluded := false
		for _, excludedColumn := range config.excludedColumns {
			if colName == excludedColumn {
				excluded = true
				break
			}
		}
		if excluded {
			continue
		}

		sourceFieldName := colName
		if config.columnNameMap != nil {
			if mappedName, ok := config.columnNameMap[colName]; ok {
				sourceFieldName = mappedName
			} else {
				return "", fmt.Errorf("destination column %s not found in column name map", colName)
			}
		}

		sourceFieldName = peerdb_clickhouse.QuoteIdentifier(sourceFieldName)

		for _, converter := range fieldExpressionConverters {
			convertedExpr, err := converter(ctx, config, sourceFieldName, field)
			if err != nil {
				return "", err
			}
			sourceFieldName = convertedExpr
		}

		selectedColumnNames = append(selectedColumnNames, sourceFieldName)
		insertedColumnNames = append(insertedColumnNames, peerdb_clickhouse.QuoteIdentifier(colName))
	}

	// Add source schema column if needed
	if sourceSchemaAsDestinationColumn {
		schemaTable, err := utils.ParseSchemaTable(config.config.WatermarkTable)
		if err != nil {
			return "", err
		}

		selectedColumnNames = append(selectedColumnNames, peerdb_clickhouse.QuoteLiteral(schemaTable.Schema))
		insertedColumnNames = append(insertedColumnNames, sourceSchemaColName)
	}

	selectorStr := strings.Join(selectedColumnNames, ",")
	insertedStr := strings.Join(insertedColumnNames, ",")
	settingsStr := buildSettingsStr(settings)

	return fmt.Sprintf("INSERT INTO %s(%s) SELECT %s FROM %s%s",
		peerdb_clickhouse.QuoteIdentifier(config.destinationTable), insertedStr, selectorStr, tableFunctionExpr, settingsStr), nil
}

// buildInsertFromTableFunctionQueryWithPartitioning builds an INSERT query with hash-based partitioning
func buildInsertFromTableFunctionQueryWithPartitioning(
	ctx context.Context,
	config *insertFromTableFunctionConfig,
	tableFunctionExpr string,
	partitionIndex uint64,
	totalPartitions uint64,
	settings map[string]string,
) (string, error) {
	var query strings.Builder

	baseQuery, err := buildInsertFromTableFunctionQuery(ctx, config, tableFunctionExpr, nil)
	if err != nil {
		return "", err
	}
	query.WriteString(baseQuery)

	if totalPartitions > 1 {

		// Get the first field for hash partitioning
		if len(config.schema.Fields) == 0 {
			return "", errors.New("schema has no fields for partitioning")
		}

		hashFieldName := config.schema.Fields[0].Name
		if config.columnNameMap != nil {
			if mappedName, ok := config.columnNameMap[hashFieldName]; ok {
				hashFieldName = mappedName
			}
		}

		whereClause := fmt.Sprintf(" WHERE cityHash64(%s) %% %d = %d",
			peerdb_clickhouse.QuoteIdentifier(hashFieldName), totalPartitions, partitionIndex)

		query.WriteString(whereClause)
	}

	query.WriteString(buildSettingsStr(settings))

	return query.String(), nil
}

// helper function to generate settings string ' SETTINGS <key1> = <val1>, <key2> = <val2>, ...'
func buildSettingsStr(settings map[string]string) string {
	if len(settings) == 0 {
		return ""
	}

	settingsList := make([]string, 0, len(settings))
	for k, v := range settings {
		settingsList = append(settingsList, fmt.Sprintf("%s = %s", k, v))
	}
	return " SETTINGS " + strings.Join(settingsList, ", ")
}
