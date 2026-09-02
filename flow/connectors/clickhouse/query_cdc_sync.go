package connclickhouse

import (
	"context"
	"errors"
	"fmt"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	chinternal "github.com/PeerDB-io/peerdb/flow/internal/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func isDeletedColNameOrDefault(configuredSoftDeleteColName string) string {
	if configuredSoftDeleteColName != "" {
		return configuredSoftDeleteColName
	}
	return defaultIsDeletedColName
}

// typedCDCTableSchema builds the QRecordSchema for the query-based CDC
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

// SyncQueryCDC replays any pending schema deltas onto the destination table,
// then converts one table's CDC records directly into a typed Avro file
// staged to S3/GCS under req.BatchID, this table's own batch sequence,
// without inserting into the final destination table. Used by the query-based
// CDC path, see flow/activities/flowable_query_cdc.go. Pair with
// NormalizeQueryCDC to insert staged batches into the final table.
func (c *ClickHouseConnector) SyncQueryCDC(
	ctx context.Context, req *model.SyncQueryCDCRequest,
) (*model.RecordTypeCounts, error) {
	if err := c.ReplayTableSchemaDeltas(
		ctx, req.Env, req.FlowJobName, []*protos.TableMapping{req.TableMapping}, req.SchemaDeltas, req.Flags,
	); err != nil {
		return nil, fmt.Errorf("failed to sync schema changes: %w", err)
	}

	schema, sourceColumnByDest := typedCDCTableSchema(req.TableSchema, req.TableMapping,
		isDeletedColNameOrDefault(req.SoftDeleteColName), versionColName)

	unboundedNumericAsString, err := internal.PeerDBEnableClickHouseNumericAsString(ctx, req.Env)
	if err != nil {
		return nil, err
	}
	numericTruncator := model.NewStreamNumericTruncator([]*protos.TableMapping{req.TableMapping}, NumericDestinationTypes)

	rowCounts := &model.RecordTypeCounts{}
	stream, err := utils.RecordsToTypedCDCStream(
		req.Records, req.TableMapping.DestinationTableIdentifier, schema, sourceColumnByDest,
		protos.DBType_CLICKHOUSE, unboundedNumericAsString, numericTruncator, rowCounts,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to convert records to typed CDC stream: %w", err)
	}

	avroSyncer := c.avroSyncMethod(req.FlowJobName, req.Env, req.Version)
	columnNameAvroFieldMap := model.ConstructColumnNameAvroFieldMap(schema.Fields)
	avroSchema, err := avroSyncer.getAvroSchema(ctx, req.Env, req.TableMapping.DestinationTableIdentifier, schema, columnNameAvroFieldMap)
	if err != nil {
		return nil, err
	}

	batchIdentifier := fmt.Sprintf("%s_%d", req.TableMapping.DestinationTableIdentifier, req.BatchID)
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

	if err := SetQueryCDCAvroStage(
		ctx, req.FlowJobName, req.TableMapping.SourceTableIdentifier, req.BatchID, avroFile, rowCounts,
	); err != nil {
		return nil, fmt.Errorf("failed to set table avro stage: %w", err)
	}

	return rowCounts, nil
}

// NormalizeQueryCDC inserts every batch in (req.StartBatchID, req.EndBatchID],
// previously staged by SyncQueryCDC, straight into the final destination
// table, one INSERT ... SELECT per batch from that batch's staged Avro file,
// deleting each stage record once applied. Returns the summed
// insert/update/delete counts across every batch actually applied.
func (c *ClickHouseConnector) NormalizeQueryCDC(
	ctx context.Context, req *model.NormalizeQueryCDCRequest,
) (*model.RecordTypeCounts, error) {
	schema, _ := typedCDCTableSchema(req.TableSchema, req.TableMapping,
		isDeletedColNameOrDefault(req.SoftDeleteColName), versionColName)
	columnNameAvroFieldMap := model.ConstructColumnNameAvroFieldMap(schema.Fields)

	var exclude []string
	if req.TableMapping != nil {
		exclude = req.TableMapping.Exclude
	}
	insertConfig := &insertFromTableFunctionConfig{
		destinationTable: req.TableMapping.DestinationTableIdentifier,
		schema:           schema,
		columnNameMap:    columnNameAvroFieldMap,
		excludedColumns:  exclude,
		config: &protos.QRepConfig{
			Env:            req.Env,
			Flags:          req.Flags,
			SourceType:     protos.DBType_BIGQUERY,
			WatermarkTable: req.TableMapping.SourceTableIdentifier,
		},
		connector: c,
		logger:    c.logger,
	}

	chSettings := chinternal.NewInsertSettings(c.chVersion, req.Version)

	rowCounts := &model.RecordTypeCounts{}
	for batchID := req.StartBatchID + 1; batchID <= req.EndBatchID; batchID++ {
		avroFile, batchCounts, err := GetQueryCDCAvroStage(ctx, req.FlowJobName, req.TableMapping.SourceTableIdentifier, batchID)
		if err != nil {
			if errors.Is(err, ErrNoAvroStage) {
				// this function is the only thing that deletes stage rows, so a missing
				// row here means a prior attempt already inserted and cleaned up this
				// batch before its checkpoint was durably recorded; safe to skip
				continue
			}
			return nil, fmt.Errorf("failed to get table avro stage for batch %d: %w", batchID, err)
		}

		stagingTableFunction, err := c.staging.TableFunctionExpr(ctx, avroFile.FilePath, stagingFormat)
		if err != nil {
			avroFile.Cleanup(ctx)
			return nil, fmt.Errorf("failed to build staging table function for batch %d: %w", batchID, err)
		}

		query, err := buildInsertFromTableFunctionQuery(ctx, insertConfig, stagingTableFunction, chSettings)
		if err != nil {
			avroFile.Cleanup(ctx)
			return nil, fmt.Errorf("failed to build insert query for %s batch %d: %w", req.TableMapping.DestinationTableIdentifier,
				batchID, err)
		}
		if err := c.exec(ctx, query); err != nil {
			avroFile.Cleanup(ctx)
			return nil, fmt.Errorf("failed to insert into %s for batch %d: %w", req.TableMapping.DestinationTableIdentifier, batchID, err)
		}
		avroFile.Cleanup(ctx)

		if err := DeleteQueryCDCAvroStage(ctx, req.FlowJobName, req.TableMapping.SourceTableIdentifier, batchID); err != nil {
			return nil, fmt.Errorf("failed to delete table avro stage for batch %d: %w", batchID, err)
		}

		rowCounts.InsertCount.Add(batchCounts.InsertCount.Load())
		rowCounts.UpdateCount.Add(batchCounts.UpdateCount.Load())
		rowCounts.DeleteCount.Add(batchCounts.DeleteCount.Load())
	}

	return rowCounts, nil
}
