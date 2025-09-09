package connclickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	peerdb_clickhouse "github.com/PeerDB-io/peerdb/flow/shared/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// InsertFromTableFunctionConfig contains the configuration for building INSERT queries from table functions
type InsertFromTableFunctionConfig struct {
	DestinationTable    string
	Schema              types.QRecordSchema
	ColumnNameMap       map[string]string // maps column names to source field names (e.g., Avro field names)
	ExcludedColumns     []string
	Config              *protos.QRepConfig
	ClickHouseConnector *ClickHouseConnector
	Logger              *slog.Logger
}

// buildInsertFromTableFunctionQuery builds a complete INSERT query from a table function expression
// This function handles column mapping, type conversions, and source schema columns
func buildInsertFromTableFunctionQuery(
	ctx context.Context,
	config *InsertFromTableFunctionConfig,
	tableFunctionExpr string,
) (string, error) {
	sourceSchemaAsDestinationColumn, err := internal.PeerDBSourceSchemaAsDestinationColumn(ctx, config.Config.Env)
	if err != nil {
		return "", err
	}

	selectedColumnNames := make([]string, 0, len(config.Schema.Fields))
	insertedColumnNames := make([]string, 0, len(config.Schema.Fields))

	for _, field := range config.Schema.Fields {
		colName := field.Name

		// Skip excluded columns
		excluded := false
		for _, excludedColumn := range config.ExcludedColumns {
			if colName == excludedColumn {
				excluded = true
				break
			}
		}
		if excluded {
			continue
		}

		// Get the source field name (e.g., from Avro schema)
		sourceFieldName := colName
		if config.ColumnNameMap != nil {
			if mappedName, ok := config.ColumnNameMap[colName]; ok {
				sourceFieldName = mappedName
			} else {
				if config.Logger != nil {
					config.Logger.Error("destination column not found in column name map",
						slog.String("columnName", colName))
				}
				return "", fmt.Errorf("destination column %s not found in column name map", colName)
			}
		}

		sourceFieldName = peerdb_clickhouse.QuoteIdentifier(sourceFieldName)

		// Apply type conversions for JSON/JSONB fields
		if (field.Type == types.QValueKindJSON || field.Type == types.QValueKindJSONB) &&
			qvalue.ShouldUseNativeJSONType(ctx, config.Config.Env, config.ClickHouseConnector.chVersion) {
			sourceFieldName = fmt.Sprintf("CAST(%s, 'JSON')", sourceFieldName)
		}

		selectedColumnNames = append(selectedColumnNames, sourceFieldName)
		insertedColumnNames = append(insertedColumnNames, peerdb_clickhouse.QuoteIdentifier(colName))
	}

	// Add source schema column if needed
	if sourceSchemaAsDestinationColumn {
		schemaTable, err := utils.ParseSchemaTable(config.Config.WatermarkTable)
		if err != nil {
			return "", err
		}

		selectedColumnNames = append(selectedColumnNames, peerdb_clickhouse.QuoteLiteral(schemaTable.Schema))
		insertedColumnNames = append(insertedColumnNames, sourceSchemaColName)
	}

	selectorStr := strings.Join(selectedColumnNames, ",")
	insertedStr := strings.Join(insertedColumnNames, ",")

	return fmt.Sprintf("INSERT INTO %s(%s) SELECT %s FROM %s",
		peerdb_clickhouse.QuoteIdentifier(config.DestinationTable), insertedStr, selectorStr, tableFunctionExpr), nil
}

// buildInsertFromTableFunctionQueryWithPartitioning builds an INSERT query with hash-based partitioning
func buildInsertFromTableFunctionQueryWithPartitioning(
	ctx context.Context,
	config *InsertFromTableFunctionConfig,
	tableFunctionExpr string,
	partitionIndex uint64,
	totalPartitions uint64,
) (string, error) {
	baseQuery, err := buildInsertFromTableFunctionQuery(ctx, config, tableFunctionExpr)
	if err != nil {
		return "", err
	}

	if totalPartitions <= 1 {
		return baseQuery + " SETTINGS throw_on_max_partitions_per_insert_block = 0", nil
	}

	// Get the first field for hash partitioning
	if len(config.Schema.Fields) == 0 {
		return "", fmt.Errorf("schema has no fields for partitioning")
	}

	hashFieldName := config.Schema.Fields[0].Name
	if config.ColumnNameMap != nil {
		if mappedName, ok := config.ColumnNameMap[hashFieldName]; ok {
			hashFieldName = mappedName
		}
	}

	whereClause := fmt.Sprintf(" WHERE cityHash64(%s) %% %d = %d",
		peerdb_clickhouse.QuoteIdentifier(hashFieldName), totalPartitions, partitionIndex)

	// Insert the WHERE clause before SETTINGS
	return baseQuery + whereClause + " SETTINGS throw_on_max_partitions_per_insert_block = 0", nil
}
