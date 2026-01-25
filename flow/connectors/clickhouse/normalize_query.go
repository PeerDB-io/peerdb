package connclickhouse

import (
	"context"
	"fmt"
	"strings"

	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	peerdb_clickhouse "github.com/PeerDB-io/peerdb/flow/pkg/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// queryVariant captures the systematic differences between the main SELECT and update SELECT.
type queryVariant struct {
	dataSource    string // JSON field to extract column data from
	isDeletedExpr string // expression for the _peerdb_is_deleted column
	versionExpr   string // expression for the _peerdb_version column
	extraWhere    string // additional WHERE conditions (if any)
}

var (
	mainQueryVariant = queryVariant{
		dataSource:    "_peerdb_data",
		isDeletedExpr: "intDiv(_peerdb_record_type, 2)",
		versionExpr:   "_peerdb_timestamp",
		extraWhere:    "",
	}

	// updateQueryVariant generates delete records for previous primary key values.
	// isDeletedExpr is hardcoded to 1 (deleted), version is decremented by 1 so
	// the delete is ordered before the latest data.
	updateQueryVariant = queryVariant{
		dataSource:    "_peerdb_match_data",
		isDeletedExpr: "1",
		versionExpr:   "_peerdb_timestamp - 1",
		extraWhere:    " AND _peerdb_match_data != '' AND _peerdb_record_type = 1",
	}
)

// resolvedColumn holds resolved column metadata after applying table mappings.
type resolvedColumn struct {
	srcName string
	dstName string
	chType  string
	qvType  types.QValueKind
}

// ColumnProjector builds SQL expressions for extracting typed values from JSON.
type ColumnProjector struct {
	env map[string]string
}

// NewColumnProjector creates a new ColumnProjector.
func NewColumnProjector(env map[string]string) *ColumnProjector {
	return &ColumnProjector{env: env}
}

// Project generates a SQL expression that extracts srcCol from dataSource and casts to chType.
func (p *ColumnProjector) Project(
	ctx context.Context,
	srcCol, dstCol string,
	colType types.QValueKind,
	chType string,
	dataSource string,
) (string, error) {
	quotedSrc := peerdb_clickhouse.QuoteLiteral(srcCol)
	quotedDst := peerdb_clickhouse.QuoteIdentifier(dstCol)

	switch chType {
	case "Date32", "Nullable(Date32)":
		return p.date32(dataSource, quotedSrc, quotedDst), nil

	case "DateTime64(6)", "Nullable(DateTime64(6))":
		return p.dateTime64(dataSource, quotedSrc, quotedDst, colType), nil

	case "Array(DateTime64(6))", "Nullable(Array(DateTime64(6)))":
		return p.arrayDateTime64(dataSource, quotedSrc, quotedDst), nil

	case "JSON", "Nullable(JSON)":
		return p.json(dataSource, quotedSrc, quotedDst), nil

	default:
		if colType == types.QValueKindBytes {
			return p.bytes(ctx, dataSource, quotedSrc, quotedDst)
		}
		return p.generic(dataSource, quotedSrc, quotedDst, chType), nil
	}
}

func (p *ColumnProjector) date32(dataSource, quotedSrc, quotedDst string) string {
	return fmt.Sprintf(
		"toDate32(parseDateTime64BestEffortOrNull(JSONExtractString(%s, %s),6,'UTC')) AS %s",
		dataSource, quotedSrc, quotedDst)
}

func (p *ColumnProjector) dateTime64(dataSource, quotedSrc, quotedDst string, colType types.QValueKind) string {
	if colType == types.QValueKindTime || colType == types.QValueKindTimeTZ {
		// parseDateTime64BestEffortOrNull for hh:mm:ss puts the year as current year
		// (or previous year if result would be in future) so explicitly anchor to unix epoch
		return fmt.Sprintf(
			"parseDateTime64BestEffortOrNull('1970-01-01 ' || JSONExtractString(%s, %s),6,'UTC') AS %s",
			dataSource, quotedSrc, quotedDst)
	}
	return fmt.Sprintf(
		"parseDateTime64BestEffortOrNull(JSONExtractString(%s, %s),6,'UTC') AS %s",
		dataSource, quotedSrc, quotedDst)
}

func (p *ColumnProjector) arrayDateTime64(dataSource, quotedSrc, quotedDst string) string {
	return fmt.Sprintf(
		"arrayMap(x -> parseDateTime64BestEffortOrNull(x,6,'UTC'),JSONExtract(%s,%s,'Array(String)')) AS %s",
		dataSource, quotedSrc, quotedDst)
}

func (p *ColumnProjector) json(dataSource, quotedSrc, quotedDst string) string {
	return fmt.Sprintf("JSONExtractString(%s, %s)::JSON AS %s", dataSource, quotedSrc, quotedDst)
}

func (p *ColumnProjector) bytes(ctx context.Context, dataSource, quotedSrc, quotedDst string) (string, error) {
	format, err := internal.PeerDBBinaryFormat(ctx, p.env)
	if err != nil {
		return "", err
	}
	switch format {
	case internal.BinaryFormatHex:
		return fmt.Sprintf("hex(base64Decode(JSONExtractString(%s, %s))) AS %s",
			dataSource, quotedSrc, quotedDst), nil
	default: // BinaryFormatRaw
		return fmt.Sprintf("base64Decode(JSONExtractString(%s, %s)) AS %s",
			dataSource, quotedSrc, quotedDst), nil
	}
}

func (p *ColumnProjector) generic(dataSource, quotedSrc, quotedDst, chType string) string {
	return fmt.Sprintf("JSONExtract(%s, %s, %s) AS %s",
		dataSource, quotedSrc, peerdb_clickhouse.QuoteLiteral(chType), quotedDst)
}

type NormalizeQueryGenerator struct {
	env                             map[string]string
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

// NewNormalizeQueryGenerator constructs a NormalizeQueryGenerator with required fields.
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
) *NormalizeQueryGenerator {
	isDeletedColumn := isDeletedColName
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
	}
}

func (t *NormalizeQueryGenerator) BuildQuery(ctx context.Context) (string, error) {
	schema := t.tableNameSchemaMapping[t.TableName]
	tableMapping := t.findTableMapping()

	columns, err := t.resolveColumns(ctx, schema, tableMapping)
	if err != nil {
		return "", err
	}

	projector := NewColumnProjector(t.env)

	// Build main SELECT
	mainSelect, colNames, err := t.buildSelect(ctx, columns, projector, mainQueryVariant)
	if err != nil {
		return "", err
	}

	selectQuery := mainSelect

	// Add UNION ALL for primary key updates
	if t.enablePrimaryUpdate {
		updateSelect, _, err := t.buildSelect(ctx, columns, projector, updateQueryVariant)
		if err != nil {
			return "", err
		}
		selectQuery += " UNION ALL " + updateSelect
	}

	colSelector := "(" + strings.Join(colNames, ",") + ") "

	t.Query = fmt.Sprintf("INSERT INTO %s %s%s%s",
		peerdb_clickhouse.QuoteIdentifier(t.TableName),
		colSelector, selectQuery, t.buildSettings())

	return t.Query, nil
}

// findTableMapping returns the table mapping for the current table, if any.
func (t *NormalizeQueryGenerator) findTableMapping() *protos.TableMapping {
	for _, tm := range t.tableMappings {
		if tm.DestinationTableIdentifier == t.TableName {
			return tm
		}
	}
	return nil
}

// resolveColumns converts schema columns into resolved columns with applied mappings.
func (t *NormalizeQueryGenerator) resolveColumns(
	ctx context.Context,
	schema *protos.TableSchema,
	tableMapping *protos.TableMapping,
) ([]resolvedColumn, error) {
	columns := make([]resolvedColumn, 0, len(schema.Columns))

	for _, col := range schema.Columns {
		rc := resolvedColumn{
			srcName: col.Name,
			dstName: col.Name,
			qvType:  types.QValueKind(col.Type),
		}

		var columnNullableEnabled bool
		if tableMapping != nil {
			for _, m := range tableMapping.Columns {
				if m.SourceName == col.Name {
					if m.DestinationName != "" {
						rc.dstName = m.DestinationName
					}
					if m.DestinationType != "" {
						// TODO basic validation to avoid injection
						rc.chType = m.DestinationType
					}
					columnNullableEnabled = m.NullableEnabled
					break
				}
			}
		}

		if rc.chType == "" {
			var err error
			rc.chType, err = qvalue.ToDWHColumnType(
				ctx, rc.qvType, t.env, protos.DBType_CLICKHOUSE,
				t.chVersion, col, schema.NullableEnabled || columnNullableEnabled,
			)
			if err != nil {
				return nil, fmt.Errorf("error while converting column type to clickhouse type: %w", err)
			}
		}

		columns = append(columns, rc)
	}

	return columns, nil
}

// buildSelect builds a complete SELECT clause for the given query variant.
// Returns the SELECT SQL and the list of column names for the column selector.
func (t *NormalizeQueryGenerator) buildSelect(
	ctx context.Context,
	columns []resolvedColumn,
	projector *ColumnProjector,
	variant queryVariant,
) (string, []string, error) {
	var projections []string
	var colNames []string

	// Build column projections
	for _, col := range columns {
		proj, err := projector.Project(ctx, col.srcName, col.dstName, col.qvType, col.chType, variant.dataSource)
		if err != nil {
			return "", nil, err
		}
		projections = append(projections, proj)
		colNames = append(colNames, peerdb_clickhouse.QuoteIdentifier(col.dstName))
	}

	// Add source schema column if enabled
	// Note: This always uses _peerdb_data regardless of variant (preserved from original behavior)
	if t.sourceSchemaAsDestinationColumn {
		projections = append(projections, fmt.Sprintf(
			"JSONExtractString(_peerdb_data, %s) AS %s",
			peerdb_clickhouse.QuoteLiteral(sourceSchemaColName),
			peerdb_clickhouse.QuoteIdentifier(sourceSchemaColName)))
		colNames = append(colNames, peerdb_clickhouse.QuoteIdentifier(sourceSchemaColName))
	}

	// Add system columns
	quotedIsDeleted := peerdb_clickhouse.QuoteIdentifier(t.isDeletedColName)
	quotedVersion := peerdb_clickhouse.QuoteIdentifier(versionColName)

	projections = append(projections,
		fmt.Sprintf("%s AS %s", variant.isDeletedExpr, quotedIsDeleted),
		fmt.Sprintf("%s AS %s", variant.versionExpr, quotedVersion))
	colNames = append(colNames, quotedIsDeleted, quotedVersion)

	// Build the full SELECT statement
	selectSQL := fmt.Sprintf("SELECT %s FROM %s WHERE %s%s",
		strings.Join(projections, ","),
		peerdb_clickhouse.QuoteIdentifier(t.rawTableName),
		t.baseWhere(),
		variant.extraWhere)

	return selectSQL, colNames, nil
}

// baseWhere returns the common WHERE clause conditions.
func (t *NormalizeQueryGenerator) baseWhere() string {
	return fmt.Sprintf(
		"_peerdb_batch_id > %d AND _peerdb_batch_id <= %d AND  _peerdb_destination_table_name = %s",
		t.lastNormBatchID, t.endBatchID, peerdb_clickhouse.QuoteLiteral(t.TableName))
}

// buildSettings returns the ClickHouse settings string for the query.
func (t *NormalizeQueryGenerator) buildSettings() string {
	chSettings := NewCHSettings(t.chVersion)
	chSettings.Add(SettingThrowOnMaxPartitionsPerInsertBlock, "0")
	chSettings.Add(SettingTypeJsonSkipDuplicatedPaths, "1")
	if t.cluster {
		chSettings.Add(SettingParallelDistributedInsertSelect, "0")
	}
	if t.version >= shared.InternalVersion_JsonEscapeDotsInKeys {
		chSettings.Add(SettingJsonTypeEscapeDotsInKeys, "1")
	}
	return chSettings.String()
}
