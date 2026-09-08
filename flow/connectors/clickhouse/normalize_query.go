package connclickhouse

import (
	"context"
	"fmt"
	"slices"
	"strings"

	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/internal/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	peerdb_clickhouse "github.com/PeerDB-io/peerdb/flow/pkg/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type NormalizeQueryGenerator struct {
	env                             map[string]string
	flags                           []string
	tableNameSchemaMapping          map[string]*protos.TableSchema
	chVersion                       *chproto.Version
	Query                           string
	TableName                       string
	rawTableName                    string
	isDeletedColName                string
	tableMappings                   []*protos.TableMapping
	lastNormBatchID                 int64
	endBatchID                      int64
	enablePrimaryUpdate             bool
	sourceSchemaAsDestinationColumn bool
	cluster                         bool
	version                         uint32
}

// NewTableNormalizeQuery constructs a TableNormalizeQuery with required fields.
func NewNormalizeQueryGenerator(
	tableName string,
	tableNameSchemaMapping map[string]*protos.TableSchema,
	tableMappings []*protos.TableMapping,
	endBatchID int64,
	lastNormBatchID int64,
	enablePrimaryUpdate bool,
	sourceSchemaAsDestinationColumn bool,
	env map[string]string,
	rawTableName string,
	chVersion *chproto.Version,
	cluster bool,
	configuredSoftDeleteColName string,
	version uint32,
	flags []string,
) *NormalizeQueryGenerator {
	isDeletedColumn := defaultIsDeletedColName
	if configuredSoftDeleteColName != "" {
		isDeletedColumn = configuredSoftDeleteColName
	}
	return &NormalizeQueryGenerator{
		TableName:                       tableName,
		tableNameSchemaMapping:          tableNameSchemaMapping,
		tableMappings:                   tableMappings,
		endBatchID:                      endBatchID,
		lastNormBatchID:                 lastNormBatchID,
		enablePrimaryUpdate:             enablePrimaryUpdate,
		sourceSchemaAsDestinationColumn: sourceSchemaAsDestinationColumn,
		env:                             env,
		rawTableName:                    rawTableName,
		chVersion:                       chVersion,
		cluster:                         cluster,
		isDeletedColName:                isDeletedColumn,
		version:                         version,
		flags:                           flags,
	}
}

// clampDates bounds a Date32 SQL expression to PeerDB's supported range,
// matching the Go-side clamp applied during initial load (processGeneralTime).
// On ClickHouse < 26.7 parseDateTime64BestEffort* already clamps, making this
// a no-op; on >= 26.7 the parse passes out-of-range values through.
func clampDates(dateExpr string) string {
	lowerBounded := fmt.Sprintf("greatest(toDate32('%d-01-01'), %s)", qvalue.ClickHouseMinYear, dateExpr)
	upperAndLowerBounded := fmt.Sprintf("least(toDate32('%d-12-31'), %s)", qvalue.ClickHouseMaxYear, lowerBounded)
	// isNull guard is required: greatest/least ignore NULL arguments on
	// current ClickHouse, which would otherwise turn NULL dates into the bound.
	return fmt.Sprintf("if(isNull(%s), NULL, %s)", dateExpr, upperAndLowerBounded)
}

var (
	minBound    = fmt.Sprintf("toDateTime64('%d-01-01 00:00:00',6,'UTC')", qvalue.ClickHouseMinYear)
	maxBound    = fmt.Sprintf("toDateTime64('%d-12-31 23:59:59.999999',6,'UTC')", qvalue.ClickHouseMaxYear)
	maxDayStart = fmt.Sprintf("toDateTime64('%d-12-31 00:00:00',6,'UTC')", qvalue.ClickHouseMaxYear)
)

// clampTimestamps bounds a DateTime64(6) SQL expression to PeerDB's supported
// range, matching the Go-side clamp applied during initial load
// (processGeneralTime): out-of-range values move to the boundary date with
// time-of-day preserved. On ClickHouse < 26.7 parseDateTime64BestEffort*
// already clamps, making this a no-op; on >= 26.7 the parse passes
// out-of-range values through.
func clampTimestamps(timestampExpr string) string {
	// Time-of-day is rebuilt from epoch microseconds instead of calendar
	// functions which misbehave on out-of-range inputs.
	timeOfDay := fmt.Sprintf("positiveModulo(toUnixTimestamp64Micro(%s), toInt64(86400000000))", timestampExpr)
	return fmt.Sprintf("multiIf(isNull(%s), NULL, %s < %s, addMicroseconds(%s, %s), %s > %s, addMicroseconds(%s, %s), %s)",
		timestampExpr,
		timestampExpr, minBound, minBound, timeOfDay,
		timestampExpr, maxBound, maxDayStart, timeOfDay,
		timestampExpr)
}

// projectionTypeKey maps a ClickHouse type declared in a TypeSystem_CH schema onto the
// spellings the projection switch in BuildQuery matches on, so that e.g. DateTime64(3),
// Nullable(DateTime('UTC')), Date or JSON(max_dynamic_paths=16) get the same tailored
// extraction as their canonical forms. Other types are returned unchanged and fall
// through to the generic JSONExtract, which casts to the declared type.
func projectionTypeKey(chType string) string {
	inner := strings.TrimSpace(chType)
	for {
		unwrapped, ok := unwrapChType(inner, "Nullable", "LowCardinality")
		if !ok {
			break
		}
		inner = unwrapped
	}
	base, args, _ := strings.Cut(inner, "(")
	switch base {
	case "DateTime64", "DateTime":
		return "DateTime64(6)"
	case "Date32", "Date":
		return "Date32"
	case "Time64", "Time":
		return "Time64(6)"
	case "JSON":
		return "JSON"
	case "Array":
		if projectionTypeKey(strings.TrimSuffix(args, ")")) == "DateTime64(6)" {
			return "Array(DateTime64(6))"
		}
	}
	return chType
}

// unwrapChType strips one level of the given wrapper types, e.g. Nullable(X) -> X
func unwrapChType(chType string, wrappers ...string) (string, bool) {
	for _, wrapper := range wrappers {
		if strings.HasPrefix(chType, wrapper+"(") && strings.HasSuffix(chType, ")") {
			return strings.TrimSpace(chType[len(wrapper)+1 : len(chType)-1]), true
		}
	}
	return chType, false
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
		// TypeSystem_CH schemas carry ClickHouse types, not QValueKinds
		var colType types.QValueKind
		if schema.System != protos.TypeSystem_CH {
			colType = types.QValueKind(column.Type)
		}

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
			if schema.System == protos.TypeSystem_CH {
				clickHouseType = column.Type
			} else {
				var err error
				clickHouseType, err = qvalue.ToDWHColumnType(
					ctx, colType, t.env, protos.DBType_CLICKHOUSE, t.chVersion, column,
					schema.NullableEnabled || columnNullableEnabled, t.flags,
				)
				if err != nil {
					return "", fmt.Errorf("error while converting column type to clickhouse type: %w", err)
				}
			}
		}

		// TypeSystem_CH schemas spell types freely (DateTime64(3), Nullable(DateTime), JSON(...)):
		// match on a canonical key so they get the same tailored extraction as the spellings
		// ToDWHColumnType produces, while the generic JSONExtract below keeps the declared type
		typeKey := clickHouseType
		if schema.System == protos.TypeSystem_CH {
			typeKey = projectionTypeKey(clickHouseType)
		}
		switch typeKey {
		case "Time64(6)", "Nullable(Time64(6))":
			fmt.Fprintf(&projection,
				"toTime64OrNull(JSONExtractString(_peerdb_data, %s), 6) AS %s,",
				peerdb_clickhouse.QuoteLiteral(colName),
				peerdb_clickhouse.QuoteIdentifier(dstColName),
			)
			if t.enablePrimaryUpdate {
				fmt.Fprintf(&projectionUpdate,
					"toTime64OrNull(JSONExtractString(_peerdb_match_data, %s), 6) AS %s,",
					peerdb_clickhouse.QuoteLiteral(colName),
					peerdb_clickhouse.QuoteIdentifier(dstColName),
				)
			}
		case "Date32", "Nullable(Date32)":
			fmt.Fprintf(&projection,
				"%s AS %s,",
				clampDates(fmt.Sprintf("toDate32(parseDateTime64BestEffortOrNull(JSONExtractString(_peerdb_data, %s),6,'UTC'))",
					peerdb_clickhouse.QuoteLiteral(colName))),
				peerdb_clickhouse.QuoteIdentifier(dstColName),
			)
			if t.enablePrimaryUpdate {
				fmt.Fprintf(&projectionUpdate,
					"%s AS %s,",
					clampDates(fmt.Sprintf("toDate32(parseDateTime64BestEffortOrNull(JSONExtractString(_peerdb_match_data, %s),6,'UTC'))",
						peerdb_clickhouse.QuoteLiteral(colName))),
					peerdb_clickhouse.QuoteIdentifier(dstColName),
				)
			}
		case "DateTime64(6)", "Nullable(DateTime64(6))":
			// Handle legacy path where TIME is stored as DateTime64 (before Time64 support)
			if colType == types.QValueKindTime || colType == types.QValueKindTimeTZ {
				time64Supported := slices.Contains(t.flags, shared.Flag_ClickHouseTime64Enabled)
				fmt.Fprintf(&projection, "%s AS %s,",
					extendedTimeToDateTime(fmt.Sprintf("JSONExtractString(_peerdb_data, %s)",
						peerdb_clickhouse.QuoteLiteral(colName)), time64Supported),
					peerdb_clickhouse.QuoteIdentifier(dstColName),
				)
				if t.enablePrimaryUpdate {
					fmt.Fprintf(&projectionUpdate, "%s AS %s,",
						extendedTimeToDateTime(fmt.Sprintf("JSONExtractString(_peerdb_match_data, %s)",
							peerdb_clickhouse.QuoteLiteral(colName)), time64Supported),
						peerdb_clickhouse.QuoteIdentifier(dstColName),
					)
				}
			} else {
				fmt.Fprintf(&projection,
					"%s AS %s,",
					clampTimestamps(fmt.Sprintf("parseDateTime64BestEffortOrNull(JSONExtractString(_peerdb_data, %s),6,'UTC')",
						peerdb_clickhouse.QuoteLiteral(colName))),
					peerdb_clickhouse.QuoteIdentifier(dstColName),
				)
				if t.enablePrimaryUpdate {
					fmt.Fprintf(&projectionUpdate,
						"%s AS %s,",
						clampTimestamps(fmt.Sprintf("parseDateTime64BestEffortOrNull(JSONExtractString(_peerdb_match_data, %s),6,'UTC')",
							peerdb_clickhouse.QuoteLiteral(colName))),
						peerdb_clickhouse.QuoteIdentifier(dstColName),
					)
				}
			}
		case "Array(DateTime64(6))", "Nullable(Array(DateTime64(6)))":
			if colType == types.QValueKindArrayTime {
				// Array-of-TIME shares ClickHouse's Array(DateTime64(6)) representation with
				// Array-of-TIMESTAMP, so it needs the same extended-time parsing as the
				// scalar TIME case above, not the timestamp best-effort parser.
				time64Supported := slices.Contains(t.flags, shared.Flag_ClickHouseTime64Enabled)
				fmt.Fprintf(&projection,
					`arrayMap(x -> %s,JSONExtract(_peerdb_data,%s,'Array(String)')) AS %s,`,
					extendedTimeToDateTime("x", time64Supported),
					peerdb_clickhouse.QuoteLiteral(colName),
					peerdb_clickhouse.QuoteIdentifier(dstColName),
				)
				if t.enablePrimaryUpdate {
					fmt.Fprintf(&projectionUpdate,
						`arrayMap(x -> %s,JSONExtract(_peerdb_match_data,%s,'Array(String)')) AS %s,`,
						extendedTimeToDateTime("x", time64Supported),
						peerdb_clickhouse.QuoteLiteral(colName),
						peerdb_clickhouse.QuoteIdentifier(dstColName),
					)
				}
			} else {
				fmt.Fprintf(&projection,
					`arrayMap(x -> %s,JSONExtract(_peerdb_data,%s,'Array(String)')) AS %s,`,
					clampTimestamps("parseDateTime64BestEffortOrNull(x,6,'UTC')"),
					peerdb_clickhouse.QuoteLiteral(colName),
					peerdb_clickhouse.QuoteIdentifier(dstColName),
				)
				if t.enablePrimaryUpdate {
					fmt.Fprintf(&projectionUpdate,
						`arrayMap(x -> %s,JSONExtract(_peerdb_match_data,%s,'Array(String)')) AS %s,`,
						clampTimestamps("parseDateTime64BestEffortOrNull(x,6,'UTC')"),
						peerdb_clickhouse.QuoteLiteral(colName),
						peerdb_clickhouse.QuoteIdentifier(dstColName),
					)
				}
			}
		case "JSON", "Nullable(JSON)":
			fmt.Fprintf(&projection,
				"JSONExtractString(_peerdb_data, %s)::JSON AS %s,",
				peerdb_clickhouse.QuoteLiteral(colName),
				peerdb_clickhouse.QuoteIdentifier(dstColName),
			)
			if t.enablePrimaryUpdate {
				fmt.Fprintf(&projectionUpdate,
					"JSONExtractString(_peerdb_match_data, %s)::JSON AS %s,",
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
	fmt.Fprintf(&projection, "intDiv(_peerdb_record_type, 2) AS %s,", peerdb_clickhouse.QuoteIdentifier(t.isDeletedColName))
	fmt.Fprintf(&colSelector, "%s,", peerdb_clickhouse.QuoteIdentifier(t.isDeletedColName))

	// add _peerdb_timestamp as _peerdb_version
	fmt.Fprintf(&projection, "_peerdb_timestamp AS %s", peerdb_clickhouse.QuoteIdentifier(versionColName))
	fmt.Fprintf(&colSelector, "%s) ", peerdb_clickhouse.QuoteIdentifier(versionColName))

	selectQuery.WriteString(projection.String())
	fmt.Fprintf(&selectQuery,
		" FROM %s WHERE _peerdb_batch_id > %d AND _peerdb_batch_id <= %d AND  _peerdb_destination_table_name = %s",
		peerdb_clickhouse.QuoteIdentifier(t.rawTableName), t.lastNormBatchID, t.endBatchID, peerdb_clickhouse.QuoteLiteral(t.TableName))

	if t.enablePrimaryUpdate {
		if t.sourceSchemaAsDestinationColumn {
			projectionUpdate.WriteString(escapedSourceSchemaSelectorFragment)
		}

		// projectionUpdate generates delete on previous record, so _peerdb_record_type is filled in as 2
		fmt.Fprintf(&projectionUpdate, "1 AS %s,", peerdb_clickhouse.QuoteIdentifier(t.isDeletedColName))
		// decrement timestamp by 1 so delete is ordered before latest data,
		// could be same if deletion records were only generated when ordering updated
		fmt.Fprintf(&projectionUpdate, "_peerdb_timestamp - 1 AS %s", peerdb_clickhouse.QuoteIdentifier(versionColName))

		selectQuery.WriteString(" UNION ALL SELECT ")
		selectQuery.WriteString(projectionUpdate.String())
		fmt.Fprintf(&selectQuery,
			" FROM %s WHERE _peerdb_match_data != '' AND _peerdb_batch_id > %d AND _peerdb_batch_id <= %d"+
				" AND  _peerdb_destination_table_name = %s AND _peerdb_record_type = 1",
			peerdb_clickhouse.QuoteIdentifier(t.rawTableName),
			t.lastNormBatchID, t.endBatchID, peerdb_clickhouse.QuoteLiteral(t.TableName))
	}

	chSettings := clickhouse.NewCHSettings(t.chVersion)
	chSettings.Add(clickhouse.SettingThrowOnMaxPartitionsPerInsertBlock, "0")
	chSettings.Add(clickhouse.SettingTypeJsonSkipDuplicatedPaths, "1")
	if t.cluster {
		chSettings.Add(clickhouse.SettingParallelDistributedInsertSelect, "0")
	}
	if t.version >= shared.InternalVersion_JsonEscapeDotsInKeys {
		chSettings.Add(clickhouse.SettingJsonTypeEscapeDotsInKeys, "1")
	}
	if t.version >= shared.InternalVersion_AlwaysUseDateTime64Inference {
		chSettings.Add(clickhouse.SettingInputFormatTryInferDates, "0")
		chSettings.Add(clickhouse.SettingInputFormatTryInferDatetimes, "1")
		chSettings.Add(clickhouse.SettingInputFormatTryInferDatetimesOnlyDatetime64, "1")
	}

	insertIntoSelectQuery := fmt.Sprintf("INSERT INTO %s %s %s%s",
		peerdb_clickhouse.QuoteIdentifier(t.TableName), colSelector.String(), selectQuery.String(), chSettings.String())

	t.Query = insertIntoSelectQuery

	return t.Query, nil
}

func extendedTimeToDateTime(jsonExtractExpr string, time64Supported bool) string {
	if time64Supported {
		return fmt.Sprintf("toDateTime64(toTime64OrNull(%s, 6), 6)", jsonExtractExpr)
	}

	// Fallback to manual string parsing for older ClickHouse versions (< 25.6)
	// that don't support toTime64OrNull(). This expression parses extended time
	// format "[-]HHH:MM:SS.xxxxxx" (e.g., "123:30:00.000000", "-1:30:00.000000")
	// by splitting on ':' and '.', computing total microseconds using integer
	// arithmetic instead of toDateTime64(<fractional_second>) to avoid precision
	// loss.
	return fmt.Sprintf(`if(length(%[1]s) > 0,
		fromUnixTimestamp64Micro(
			(if(startsWith(%[1]s, '-'), -1, 1)) *
			(toInt64(splitByChar(':', if(startsWith(%[1]s, '-'), substring(%[1]s, 2), %[1]s))[1]) * 3600 * 1000000 +
			 toInt64(splitByChar(':', if(startsWith(%[1]s, '-'), substring(%[1]s, 2), %[1]s))[2]) * 60 * 1000000 +
			 toInt64(splitByChar('.', splitByChar(':', if(startsWith(%[1]s, '-'), substring(%[1]s, 2), %[1]s))[3])[1]) * 1000000 +
			 toInt64(splitByChar('.', splitByChar(':', if(startsWith(%[1]s, '-'), substring(%[1]s, 2), %[1]s))[3])[2]))
		),
		NULL)`, jsonExtractExpr)
}
