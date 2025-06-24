package connclickhouse

import (
	"context"
	"fmt"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	peerdb_clickhouse "github.com/PeerDB-io/peerdb/flow/shared/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type NormalizeQueryGenerator struct {
	tableNameSchemaMapping          map[string]*protos.TableSchema
	env                             map[string]string
	Query                           string
	TableName                       string
	rawTableName                    string
	tableMappings                   []*protos.TableMapping
	Part                            uint64
	syncBatchID                     int64
	batchIDToLoadForTable           int64
	numParts                        uint64
	enablePrimaryUpdate             bool
	sourceSchemaAsDestinationColumn bool
}

// NewTableNormalizeQuery constructs a TableNormalizeQuery with required fields.
func NewNormalizeQueryGenerator(
	tableName string,
	part uint64,
	tableNameSchemaMapping map[string]*protos.TableSchema,
	tableMappings []*protos.TableMapping,
	syncBatchID int64,
	batchIDToLoadForTable int64,
	numParts uint64,
	enablePrimaryUpdate bool,
	sourceSchemaAsDestinationColumn bool,
	env map[string]string,
	rawTableName string,
) *NormalizeQueryGenerator {
	return &NormalizeQueryGenerator{
		TableName:                       tableName,
		Part:                            part,
		tableNameSchemaMapping:          tableNameSchemaMapping,
		tableMappings:                   tableMappings,
		syncBatchID:                     syncBatchID,
		batchIDToLoadForTable:           batchIDToLoadForTable,
		numParts:                        numParts,
		enablePrimaryUpdate:             enablePrimaryUpdate,
		sourceSchemaAsDestinationColumn: sourceSchemaAsDestinationColumn,
		env:                             env,
		rawTableName:                    rawTableName,
	}
}

func (t *NormalizeQueryGenerator) BuildQuery(ctx context.Context) (string, error) {
	selectQuery := strings.Builder{}
	selectQuery.WriteString("SELECT ")

	colSelector := strings.Builder{}
	colSelector.WriteByte('(')

	schema := t.tableNameSchemaMapping[t.TableName]

	var tableMapping *protos.TableMapping
	for _, tm := range t.tableMappings {
		if tm.DestinationTableIdentifier == t.TableName {
			tableMapping = tm
			break
		}
	}

	var escapedSourceSchemaSelectorFragment string
	if t.sourceSchemaAsDestinationColumn {
		escapedSourceSchemaSelectorFragment = fmt.Sprintf("JSONExtractString(_peerdb_data, %s) AS %s,",
			peerdb_clickhouse.QuoteLiteral(sourceSchemaColName),
			peerdb_clickhouse.QuoteIdentifier(sourceSchemaColName))
	}

	projection := strings.Builder{}
	projectionUpdate := strings.Builder{}

	for _, column := range schema.Columns {
		colName := column.Name
		dstColName := colName
		colType := types.QValueKind(column.Type)

		var clickHouseType string
		var columnNullableEnabled bool
		if tableMapping != nil {
			for _, col := range tableMapping.Columns {
				if col.SourceName == colName {
					if col.DestinationName != "" {
						dstColName = col.DestinationName
					}
					if col.DestinationType != "" {
						// TODO basic validation to avoid injection
						clickHouseType = col.DestinationType
					}
					columnNullableEnabled = col.NullableEnabled
					break
				}
			}
		}

		fmt.Fprintf(&colSelector, "%s,", peerdb_clickhouse.QuoteIdentifier(dstColName))
		if clickHouseType == "" {
			var err error
			clickHouseType, err = qvalue.ToDWHColumnType(
				ctx, colType, t.env, protos.DBType_CLICKHOUSE, column, schema.NullableEnabled || columnNullableEnabled,
			)
			if err != nil {
				return "", fmt.Errorf("error while converting column type to clickhouse type: %w", err)
			}
		}

		switch clickHouseType {
		case "Date32", "Nullable(Date32)":
			fmt.Fprintf(&projection,
				"toDate32(parseDateTime64BestEffortOrNull(JSONExtractString(_peerdb_data, %s),6)) AS %s,",
				peerdb_clickhouse.QuoteLiteral(colName),
				peerdb_clickhouse.QuoteIdentifier(dstColName),
			)
			if t.enablePrimaryUpdate {
				fmt.Fprintf(&projectionUpdate,
					"toDate32(parseDateTime64BestEffortOrNull(JSONExtractString(_peerdb_match_data, %s),6)) AS %s,",
					peerdb_clickhouse.QuoteLiteral(colName),
					peerdb_clickhouse.QuoteIdentifier(dstColName),
				)
			}
		case "DateTime64(6)", "Nullable(DateTime64(6))":
			if colType == types.QValueKindTime || colType == types.QValueKindTimeTZ {
				// parseDateTime64BestEffortOrNull for hh:mm:ss puts the year as current year
				// (or previous year if result would be in future) so explicitly anchor to unix epoch
				fmt.Fprintf(&projection,
					"parseDateTime64BestEffortOrNull('1970-01-01 ' || JSONExtractString(_peerdb_data, %s),6) AS %s,",
					peerdb_clickhouse.QuoteLiteral(colName),
					peerdb_clickhouse.QuoteIdentifier(dstColName),
				)
				if t.enablePrimaryUpdate {
					fmt.Fprintf(&projectionUpdate,
						"parseDateTime64BestEffortOrNull('1970-01-01 ' || JSONExtractString(_peerdb_match_data, %s),6) AS %s,",
						peerdb_clickhouse.QuoteLiteral(colName),
						peerdb_clickhouse.QuoteIdentifier(dstColName),
					)
				}
			} else {
				fmt.Fprintf(&projection,
					"parseDateTime64BestEffortOrNull(JSONExtractString(_peerdb_data, %s),6) AS %s,",
					peerdb_clickhouse.QuoteLiteral(colName),
					peerdb_clickhouse.QuoteIdentifier(dstColName),
				)
				if t.enablePrimaryUpdate {
					fmt.Fprintf(&projectionUpdate,
						"parseDateTime64BestEffortOrNull(JSONExtractString(_peerdb_match_data, %s),6) AS %s,",
						peerdb_clickhouse.QuoteLiteral(colName),
						peerdb_clickhouse.QuoteIdentifier(dstColName),
					)
				}
			}
		case "Array(DateTime64(6))", "Nullable(Array(DateTime64(6)))":
			fmt.Fprintf(&projection,
				`arrayMap(x -> parseDateTime64BestEffortOrNull(trimBoth(x, '"'), 6), JSONExtractArrayRaw(_peerdb_data, %s)) AS %s,`,
				peerdb_clickhouse.QuoteLiteral(colName),
				peerdb_clickhouse.QuoteIdentifier(dstColName),
			)
			if t.enablePrimaryUpdate {
				fmt.Fprintf(&projectionUpdate,
					`arrayMap(x -> parseDateTime64BestEffortOrNull(trimBoth(x, '"'), 6), JSONExtractArrayRaw(_peerdb_match_data, %s)) AS %s,`,
					peerdb_clickhouse.QuoteLiteral(colName),
					peerdb_clickhouse.QuoteIdentifier(dstColName),
				)
			}
		default:
			projLen := projection.Len()
			if colType == types.QValueKindBytes {
				format, err := internal.PeerDBBinaryFormat(ctx, t.env)
				if err != nil {
					return "", err
				}
				switch format {
				case internal.BinaryFormatRaw:
					fmt.Fprintf(&projection,
						"base64Decode(JSONExtractString(_peerdb_data, %s)) AS %s,",
						peerdb_clickhouse.QuoteLiteral(colName),
						peerdb_clickhouse.QuoteIdentifier(dstColName),
					)
					if t.enablePrimaryUpdate {
						fmt.Fprintf(&projectionUpdate,
							"base64Decode(JSONExtractString(_peerdb_match_data, %s)) AS %s,",
							peerdb_clickhouse.QuoteLiteral(colName),
							peerdb_clickhouse.QuoteIdentifier(dstColName),
						)
					}
				case internal.BinaryFormatHex:
					fmt.Fprintf(&projection, "hex(base64Decode(JSONExtractString(_peerdb_data, %s))) AS %s,",
						peerdb_clickhouse.QuoteLiteral(colName),
						peerdb_clickhouse.QuoteIdentifier(dstColName),
					)
					if t.enablePrimaryUpdate {
						fmt.Fprintf(&projectionUpdate,
							"hex(base64Decode(JSONExtractString(_peerdb_match_data, %s))) AS %s,",
							peerdb_clickhouse.QuoteLiteral(colName),
							peerdb_clickhouse.QuoteIdentifier(dstColName),
						)
					}
				}
			}

			// proceed with default logic if logic above didn't add any sql
			if projection.Len() == projLen {
				fmt.Fprintf(
					&projection,
					"JSONExtract(_peerdb_data, %s, %s) AS %s,",
					peerdb_clickhouse.QuoteLiteral(colName),
					peerdb_clickhouse.QuoteLiteral(clickHouseType),
					peerdb_clickhouse.QuoteIdentifier(dstColName),
				)
				if t.enablePrimaryUpdate {
					fmt.Fprintf(
						&projectionUpdate,
						"JSONExtract(_peerdb_match_data, %s, %s) AS %s,",
						peerdb_clickhouse.QuoteLiteral(colName),
						peerdb_clickhouse.QuoteLiteral(clickHouseType),
						peerdb_clickhouse.QuoteIdentifier(dstColName),
					)
				}
			}
		}
	}

	if t.sourceSchemaAsDestinationColumn {
		projection.WriteString(escapedSourceSchemaSelectorFragment)
		fmt.Fprintf(&colSelector, "%s,", peerdb_clickhouse.QuoteIdentifier(sourceSchemaColName))
	}

	// add _peerdb_sign as _peerdb_record_type / 2
	fmt.Fprintf(&projection, "intDiv(_peerdb_record_type, 2) AS %s,", peerdb_clickhouse.QuoteIdentifier(signColName))
	fmt.Fprintf(&colSelector, "%s,", peerdb_clickhouse.QuoteIdentifier(signColName))

	// add _peerdb_timestamp as _peerdb_version
	fmt.Fprintf(&projection, "_peerdb_timestamp AS %s", peerdb_clickhouse.QuoteIdentifier(versionColName))
	fmt.Fprintf(&colSelector, "%s) ", peerdb_clickhouse.QuoteIdentifier(versionColName))

	selectQuery.WriteString(projection.String())
	fmt.Fprintf(&selectQuery,
		" FROM %s WHERE _peerdb_batch_id > %d AND _peerdb_batch_id <= %d AND  _peerdb_destination_table_name = %s",
		peerdb_clickhouse.QuoteIdentifier(t.rawTableName), t.batchIDToLoadForTable, t.syncBatchID, peerdb_clickhouse.QuoteLiteral(t.TableName))
	if t.numParts > 1 {
		fmt.Fprintf(&selectQuery, " AND cityHash64(_peerdb_uid) %% %d = %d", t.numParts, t.Part)
	}

	if t.enablePrimaryUpdate {
		if t.sourceSchemaAsDestinationColumn {
			projectionUpdate.WriteString(escapedSourceSchemaSelectorFragment)
		}

		// projectionUpdate generates delete on previous record, so _peerdb_record_type is filled in as 2
		fmt.Fprintf(&projectionUpdate, "1 AS %s,", peerdb_clickhouse.QuoteIdentifier(signColName))
		// decrement timestamp by 1 so delete is ordered before latest data,
		// could be same if deletion records were only generated when ordering updated
		fmt.Fprintf(&projectionUpdate, "_peerdb_timestamp - 1 AS %s", peerdb_clickhouse.QuoteIdentifier(versionColName))

		selectQuery.WriteString(" UNION ALL SELECT ")
		selectQuery.WriteString(projectionUpdate.String())
		fmt.Fprintf(&selectQuery,
			" FROM %s WHERE _peerdb_match_data != '' AND _peerdb_batch_id > %d AND _peerdb_batch_id <= %d"+
				" AND  _peerdb_destination_table_name = %s AND _peerdb_record_type = 1",
			peerdb_clickhouse.QuoteIdentifier(t.rawTableName),
			t.batchIDToLoadForTable, t.syncBatchID, peerdb_clickhouse.QuoteLiteral(t.TableName))
		if t.numParts > 1 {
			fmt.Fprintf(&selectQuery, " AND cityHash64(_peerdb_uid) %% %d = %d", t.numParts, t.Part)
		}
	}

	insertIntoSelectQuery := fmt.Sprintf("INSERT INTO %s %s %s",
		peerdb_clickhouse.QuoteIdentifier(t.TableName), colSelector.String(), selectQuery.String())

	t.Query = insertIntoSelectQuery

	return t.Query, nil
}
