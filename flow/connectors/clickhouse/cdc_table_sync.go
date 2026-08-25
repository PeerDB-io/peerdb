package connclickhouse

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	chinternal "github.com/PeerDB-io/peerdb/flow/internal/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// typedCDCTableSchema builds the QRecordSchema for the isolated per-table CDC
// path's typed Avro file
func typedCDCTableSchema(
	tableSchema *protos.TableSchema, tableMapping *protos.TableMapping, isDeletedColName, versionColName string,
) (types.QRecordSchema, map[string]string) {
	fields := make([]types.QField, 0, len(tableSchema.Columns)+2)
	sourceColumnByDest := make(map[string]string, len(tableSchema.Columns))
	for _, column := range tableSchema.Columns {
		dstColName := column.Name
		if tableMapping != nil {
			for _, col := range tableMapping.Columns {
				if col.SourceName == column.Name {
					if col.DestinationName != "" {
						dstColName = col.DestinationName
					}
					break
				}
			}
		}
		sourceColumnByDest[dstColName] = column.Name
		fields = append(fields, types.QField{
			Name:     dstColName,
			Type:     types.QValueKind(column.Type),
			Nullable: column.Nullable,
		})
	}
	fields = append(fields,
		types.QField{Name: isDeletedColName, Type: types.QValueKindInt64},
		types.QField{Name: versionColName, Type: types.QValueKindInt64},
	)
	return types.QRecordSchema{Fields: fields}, sourceColumnByDest
}

// SyncTableCDC converts one table's CDC records directly into a typed Avro
// file staged to S3/GCS under req.BatchID, this table's own batch sequence,
// without touching the final destination table. Used by the isolated
// per-table CDC path, see flow/activities/flowable_isolated_cdc.go. Pair with
// NormalizeTableCDC to insert staged batches into the final table.
func (c *ClickHouseConnector) SyncTableCDC(
	ctx context.Context, req *model.SyncTableCDCRequest,
) (*model.RecordTypeCounts, error) {
	schema, sourceColumnByDest := typedCDCTableSchema(req.TableSchema, req.TableMapping, defaultIsDeletedColName, versionColName)

	unboundedNumericAsString, err := internal.PeerDBEnableClickHouseNumericAsString(ctx, req.Env)
	if err != nil {
		return nil, err
	}
	numericTruncator := model.NewStreamNumericTruncator([]*protos.TableMapping{req.TableMapping}, NumericDestinationTypes)

	rowCounts := &model.RecordTypeCounts{}
	stream, err := utils.RecordsToTypedCDCStream(
		req.Records, req.DestinationTableIdentifier, schema, sourceColumnByDest,
		protos.DBType_CLICKHOUSE, unboundedNumericAsString, numericTruncator, rowCounts,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to convert records to typed CDC stream: %w", err)
	}

	avroSyncer := c.avroSyncMethod(req.FlowJobName, req.Env, req.Version)
	columnNameAvroFieldMap := model.ConstructColumnNameAvroFieldMap(schema.Fields)
	avroSchema, err := avroSyncer.getAvroSchema(ctx, req.Env, req.DestinationTableIdentifier, schema, columnNameAvroFieldMap)
	if err != nil {
		return nil, err
	}

	batchIdentifier := fmt.Sprintf("%s_%s_%d", common.RandomString(16), req.DestinationTableIdentifier, req.BatchID)
	avroFile, err := avroSyncer.writeToAvroFile(ctx, req.Env, stream, nil, avroSchema, batchIdentifier, req.FlowJobName, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to write typed CDC avro file: %w", err)
	}

	// rowCounts is fully populated once writeToAvroFile (which drains stream to
	// completion) returns.
	if avroFile.NumRecords == 0 {
		avroFile.Cleanup(ctx)
		return rowCounts, nil
	}

	if err := SetTableAvroStage(ctx, req.FlowJobName, req.SourceTableIdentifier, req.BatchID, avroFile); err != nil {
		return nil, fmt.Errorf("failed to set table avro stage: %w", err)
	}

	return rowCounts, nil
}

// NormalizeTableCDC inserts every batch in (req.StartBatchID, req.EndBatchID],
// previously staged by SyncTableCDC, straight into the final destination
// table, one INSERT ... SELECT per batch from that batch's staged Avro file,
// deleting each stage record once applied.
func (c *ClickHouseConnector) NormalizeTableCDC(ctx context.Context, req *model.NormalizeTableCDCRequest) error {
	schema, _ := typedCDCTableSchema(req.TableSchema, req.TableMapping, defaultIsDeletedColName, versionColName)
	columnNameAvroFieldMap := model.ConstructColumnNameAvroFieldMap(schema.Fields)

	var exclude []string
	if req.TableMapping != nil {
		exclude = req.TableMapping.Exclude
	}
	insertConfig := &insertFromTableFunctionConfig{
		destinationTable: req.DestinationTableIdentifier,
		schema:           schema,
		columnNameMap:    columnNameAvroFieldMap,
		excludedColumns:  exclude,
		config: &protos.QRepConfig{
			Env:            req.Env,
			Flags:          req.Flags,
			SourceType:     protos.DBType_BIGQUERY,
			WatermarkTable: req.SourceTableIdentifier,
		},
		connector: c,
		logger:    c.logger,
	}

	chSettings := chinternal.NewInsertSettings(c.chVersion, req.Version)

	for batchID := req.StartBatchID + 1; batchID <= req.EndBatchID; batchID++ {
		avroFile, err := GetTableAvroStage(ctx, req.FlowJobName, req.SourceTableIdentifier, batchID)
		if err != nil {
			return fmt.Errorf("failed to get table avro stage for batch %d: %w", batchID, err)
		}

		stagingTableFunction, err := c.staging.TableFunctionExpr(ctx, avroFile.FilePath, stagingFormat)
		if err != nil {
			avroFile.Cleanup(ctx)
			return fmt.Errorf("failed to build staging table function for batch %d: %w", batchID, err)
		}

		query, err := buildInsertFromTableFunctionQuery(ctx, insertConfig, stagingTableFunction, chSettings)
		if err != nil {
			avroFile.Cleanup(ctx)
			return fmt.Errorf("failed to build insert query for %s batch %d: %w", req.DestinationTableIdentifier, batchID, err)
		}
		if err := c.exec(ctx, query); err != nil {
			avroFile.Cleanup(ctx)
			return fmt.Errorf("failed to insert into %s for batch %d: %w", req.DestinationTableIdentifier, batchID, err)
		}
		avroFile.Cleanup(ctx)

		if err := DeleteTableAvroStage(ctx, req.FlowJobName, req.SourceTableIdentifier, batchID); err != nil {
			return fmt.Errorf("failed to delete table avro stage for batch %d: %w", batchID, err)
		}
	}

	return nil
}
