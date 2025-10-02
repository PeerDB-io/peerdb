package connclickhouse

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/shopspring/decimal"
	"golang.org/x/sync/errgroup"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/shared"
	peerdb_clickhouse "github.com/PeerDB-io/peerdb/flow/shared/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

const (
	checkIfTableExistsSQL = `SELECT exists(SELECT 1 FROM system.tables WHERE database = %s AND name = %s) AS table_exists`
	dropTableIfExistsSQL  = "DROP TABLE IF EXISTS %s%s"
	rawColumns            = `(
		_peerdb_uid UUID,
		_peerdb_timestamp Int64,
		_peerdb_destination_table_name String,
		_peerdb_data String,
		_peerdb_record_type Int,
		_peerdb_match_data String,
		_peerdb_batch_id Int64,
		_peerdb_unchanged_toast_columns String
	)`
	zooPathPrefix = "/clickhouse/tables/{uuid}/{shard}/{database}/%s"
)

// GetRawTableName returns the raw table name for the given table identifier.
func (c *ClickHouseConnector) GetRawTableName(flowJobName string) string {
	return "_peerdb_raw_" + shared.ReplaceIllegalCharactersWithUnderscores(flowJobName)
}

func (c *ClickHouseConnector) checkIfTableExists(ctx context.Context, databaseName string, tableIdentifier string) (bool, error) {
	var result uint8
	if err := c.queryRow(ctx,
		fmt.Sprintf(checkIfTableExistsSQL, peerdb_clickhouse.QuoteLiteral(databaseName), peerdb_clickhouse.QuoteLiteral(tableIdentifier)),
	).Scan(&result); err != nil {
		return false, fmt.Errorf("error while reading result row: %w", err)
	}

	return result == 1, nil
}

func (c *ClickHouseConnector) CreateRawTable(ctx context.Context, req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	var rawDistributedName string
	rawTableName := c.GetRawTableName(req.FlowJobName)
	engine := "MergeTree()"
	if c.Config.Replicated {
		engine = fmt.Sprintf(
			"ReplicatedMergeTree('%s%s','{replica}')",
			zooPathPrefix,
			peerdb_clickhouse.EscapeStr(rawTableName),
		)
	}
	onCluster := c.onCluster()
	if onCluster != "" {
		rawDistributedName = rawTableName
		rawTableName += "_shard"
	}

	createRawTableSQL := `CREATE TABLE IF NOT EXISTS %s%s %s ENGINE = %s ORDER BY (_peerdb_batch_id, _peerdb_destination_table_name)`
	if err := c.execWithLogging(ctx,
		fmt.Sprintf(createRawTableSQL, peerdb_clickhouse.QuoteIdentifier(rawTableName), onCluster, rawColumns, engine),
	); err != nil {
		return nil, fmt.Errorf("unable to create raw table: %w", err)
	}

	if onCluster != "" {
		createRawDistributedSQL := `CREATE TABLE IF NOT EXISTS %s%s %s ENGINE = Distributed(%s,%s,%s,cityHash64(_peerdb_uid))`
		if err := c.execWithLogging(ctx,
			fmt.Sprintf(createRawDistributedSQL, peerdb_clickhouse.QuoteIdentifier(rawDistributedName), onCluster,
				rawColumns,
				peerdb_clickhouse.QuoteIdentifier(c.Config.Cluster),
				peerdb_clickhouse.QuoteIdentifier(c.Config.Database),
				peerdb_clickhouse.QuoteIdentifier(rawTableName)),
		); err != nil {
			return nil, fmt.Errorf("unable to create raw table: %w", err)
		}
	}

	return &protos.CreateRawTableOutput{
		TableIdentifier: rawTableName,
	}, nil
}

func (c *ClickHouseConnector) avroSyncMethod(flowJobName string, env map[string]string, version uint32) *ClickHouseAvroSyncMethod {
	qrepConfig := &protos.QRepConfig{
		StagingPath:                c.credsProvider.BucketPath,
		FlowJobName:                flowJobName,
		DestinationTableIdentifier: c.GetRawTableName(flowJobName),
		Env:                        env,
		Version:                    version,
	}
	return NewClickHouseAvroSyncMethod(qrepConfig, c)
}

func (c *ClickHouseConnector) syncRecordsViaAvro(
	ctx context.Context,
	req *model.SyncRecordsRequest[model.RecordItems],
	syncBatchID int64,
) (*model.SyncResponse, error) {
	tableNameRowsMapping := utils.InitialiseTableRowsMap(req.TableMappings)
	unboundedNumericAsString, err := internal.PeerDBEnableClickHouseNumericAsString(ctx, req.Env)
	if err != nil {
		return nil, err
	}
	streamReq := model.NewRecordsToStreamRequest(
		req.Records.GetRecords(), tableNameRowsMapping, syncBatchID, unboundedNumericAsString,
		protos.DBType_CLICKHOUSE,
	)
	numericTruncator := model.NewStreamNumericTruncator(req.TableMappings, peerdb_clickhouse.NumericDestinationTypes)
	stream, err := utils.RecordsToRawTableStream(streamReq, numericTruncator)
	if err != nil {
		return nil, fmt.Errorf("failed to convert records to raw table stream: %w", err)
	}

	avroSyncer := c.avroSyncMethod(req.FlowJobName, req.Env, req.Version)
	numRecords, err := avroSyncer.SyncRecords(ctx, req.Env, stream, req.FlowJobName, syncBatchID)
	if err != nil {
		return nil, err
	}
	warnings := numericTruncator.Warnings()

	if err := c.ReplayTableSchemaDeltas(ctx, req.Env, req.FlowJobName, req.TableMappings, req.Records.SchemaDeltas); err != nil {
		return nil, fmt.Errorf("failed to sync schema changes: %w", err)
	}

	return &model.SyncResponse{
		LastSyncedCheckpoint: req.Records.GetLastCheckpoint(),
		NumRecordsSynced:     numRecords,
		CurrentSyncBatchID:   syncBatchID,
		TableNameRowsMapping: tableNameRowsMapping,
		TableSchemaDeltas:    req.Records.SchemaDeltas,
		Warnings:             warnings,
	}, nil
}

func formatSlice[T any](values []T, f func(T) string) string {
	s := make([]string, 0, len(values))
	for _, value := range values {
		s = append(s, f(value))
	}
	return fmt.Sprintf("[%s]", strings.Join(s, ","))
}

func formatQValue(value types.QValue, nullable bool) string {
	switch v := value.(type) {
	case nil:
		return "NULL"
	case types.QValueNull:
		if !nullable {
			switch types.QValueKind(v) {
			case types.QValueKindArrayFloat32,
				types.QValueKindArrayFloat64,
				types.QValueKindArrayInt16,
				types.QValueKindArrayInt32,
				types.QValueKindArrayInt64,
				types.QValueKindArrayString,
				types.QValueKindArrayEnum,
				types.QValueKindArrayDate,
				types.QValueKindArrayInterval,
				types.QValueKindArrayTimestamp,
				types.QValueKindArrayTimestampTZ,
				types.QValueKindArrayBoolean,
				types.QValueKindArrayJSON,
				types.QValueKindArrayJSONB,
				types.QValueKindArrayUUID,
				types.QValueKindArrayNumeric:
				return "[]"
			case types.QValueKindFloat32,
				types.QValueKindFloat64,
				types.QValueKindInt8,
				types.QValueKindInt16,
				types.QValueKindInt32,
				types.QValueKindInt64,
				types.QValueKindInt256,
				types.QValueKindUInt8,
				types.QValueKindUInt16,
				types.QValueKindUInt32,
				types.QValueKindUInt64,
				types.QValueKindUInt256,
				types.QValueKindBoolean,
				types.QValueKindNumeric:
				return "0"
			case types.QValueKindQChar,
				types.QValueKindString,
				types.QValueKindEnum,
				types.QValueKindBytes:
				return "''"
			case types.QValueKindJSON,
				types.QValueKindJSONB:
				return "'{}'"
			case types.QValueKindUUID:
				return "'00000000-0000-0000-0000-000000000000'"
			case types.QValueKindDate:
				return "'1970-01-01'"
			case types.QValueKindTimestamp, types.QValueKindTimestampTZ:
				return "'1970-01-01 00:00:00'"
			}
		}
		return "NULL"
	case types.QValueArrayBoolean:
		return formatSlice(v.Val, func(val bool) string {
			if val {
				return "true"
			} else {
				return "false"
			}
		})
	case types.QValueArrayInt16:
		return formatSlice(v.Val, func(val int16) string {
			return strconv.FormatInt(int64(val), 10)
		})
	case types.QValueArrayInt32:
		return formatSlice(v.Val, func(val int32) string {
			return strconv.FormatInt(int64(val), 10)
		})
	case types.QValueArrayInt64:
		return formatSlice(v.Val, func(val int64) string {
			return strconv.FormatInt(val, 10)
		})
	case types.QValueArrayString:
		return formatSlice(v.Val, peerdb_clickhouse.QuoteLiteral)
	case types.QValueArrayFloat32:
		return formatSlice(v.Val, func(val float32) string {
			return strconv.FormatFloat(float64(val), 'g', -1, 32)
		})
	case types.QValueArrayFloat64:
		return formatSlice(v.Val, func(val float64) string {
			return strconv.FormatFloat(val, 'g', -1, 64)
		})
	case types.QValueArrayNumeric:
		return formatSlice(v.Val, func(val decimal.Decimal) string {
			return val.String()
		})
	case types.QValueArrayDate:
		return formatSlice(v.Val, func(val time.Time) string {
			return val.Format(time.DateOnly)
		})
	case types.QValueArrayTimestamp:
		return formatSlice(v.Val, func(val time.Time) string {
			return val.Format(time.StampNano)
		})
	case types.QValueArrayTimestampTZ:
		return formatSlice(v.Val, func(val time.Time) string {
			return val.Format(time.StampNano)
		})
	case types.QValueArrayEnum:
		return formatSlice(v.Val, peerdb_clickhouse.QuoteLiteral)
	case types.QValueBoolean:
		if v.Val {
			return "true"
		} else {
			return "false"
		}
	case types.QValueInt16:
		return strconv.FormatInt(int64(v.Val), 10)
	case types.QValueInt32:
		return strconv.FormatInt(int64(v.Val), 10)
	case types.QValueInt64:
		return strconv.FormatInt(v.Val, 10)
	case types.QValueString:
		return peerdb_clickhouse.QuoteLiteral(v.Val)
	case types.QValueFloat32:
		return strconv.FormatFloat(float64(v.Val), 'g', -1, 32)
	case types.QValueFloat64:
		return strconv.FormatFloat(v.Val, 'g', -1, 64)
	case types.QValueNumeric:
		return v.Val.String()
	case types.QValueDate:
		return v.Val.Format(time.DateOnly)
	case types.QValueTimestamp:
		return v.Val.Format(time.StampNano)
	case types.QValueTimestampTZ:
		return v.Val.Format(time.StampNano)
	case types.QValueEnum:
		return peerdb_clickhouse.QuoteLiteral(v.Val)
	case types.QValueBytes:
		return peerdb_clickhouse.QuoteLiteral(shared.UnsafeFastReadOnlyBytesToString(v.Val))
	default:
		return peerdb_clickhouse.QuoteLiteral(fmt.Sprint(v.Value()))
	}
}

func (c *ClickHouseConnector) SyncRecords(ctx context.Context, req *model.SyncRecordsRequest[model.RecordItems]) (*model.SyncResponse, error) {
	enableStream, err := internal.PeerDBEnableClickHouseStream(ctx, req.Env)
	if err != nil {
		return nil, err
	} else if enableStream {
		var numRecords int64
		const (
			queryTypeNone int8 = iota
			queryTypeInsert
			queryTypeUpdate
			queryTypeDelete
		)
		type query struct {
			table        string
			values       []string
			columns      []string
			whereValues  []string
			whereColumns []string
			lsn          int64
			ty           int8
		}
		var lsns [4]atomic.Int64
		var queries [4]chan query
		for workerId := range len(queries) {
			queries[workerId] = make(chan query)
		}
		tableNameRowsMapping := utils.InitialiseTableRowsMap(req.TableMappings)
		flushLoopDone := make(chan struct{})
		go func() {
			flushTimeout, err := internal.PeerDBQueueFlushTimeoutSeconds(ctx, req.Env)
			if err != nil {
				c.logger.Warn("failed to get flush timeout, no periodic flushing", slog.Any("error", err))
				return
			}
			ticker := time.NewTicker(flushTimeout)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-flushLoopDone:
					return
				// flush loop doesn't block processing new messages
				case <-ticker.C:
					oldLastSeen := req.ConsumedOffset.Load()
					lastSeen := oldLastSeen
					for workerId := range len(lsns) {
						lastSeen = min(lastSeen, lsns[workerId].Load())
					}
					if lastSeen > oldLastSeen {
						if err := c.SetLastOffset(ctx, req.FlowJobName, model.CdcCheckpoint{ID: lastSeen}); err != nil {
							c.logger.Warn("SetLastOffset error", slog.Any("error", err))
						} else {
							shared.AtomicInt64Max(req.ConsumedOffset, lastSeen)
							c.logger.Info("processBatch", slog.Int64("updated last offset", lastSeen))
						}
					}
				}
			}
		}()
		defer close(flushLoopDone)

		// TODO sourceSchemaAsDestinationColumn
		// TODO schema changes
		fnvHash := fnv.New64a()
		group, groupCtx := errgroup.WithContext(ctx)
		for workerId := range len(lsns) {
			group.Go(func() error {
				currentType := queryTypeNone
				currentTable := ""
				batchTimeout := make(<-chan time.Time)
				var batch []query
				finishBatch := func() error {
					c.logger.Info("finishBatch", slog.Int("len", len(batch)))
					if len(batch) > 0 {
						switch currentType {
						case queryTypeInsert:
							columns := make([]string, 0, len(batch[0].columns))
							for _, colName := range batch[0].columns {
								col := peerdb_clickhouse.QuoteIdentifier(colName)
								columns = append(columns, col)
							}
							baseQuery := fmt.Sprintf("INSERT INTO %s(%s) VALUES ",
								peerdb_clickhouse.QuoteIdentifier(currentTable),
								strings.Join(columns, ","),
							)
							// batch in inserts of 100KB
							var batchIdx int
							for batchIdx != -1 {
								querySize := len(baseQuery)
								finalValues := make([]string, 0, len(batch))
								for idx, q := range batch[batchIdx:] {
									finalValue := fmt.Sprintf("(%s)", strings.Join(q.values, ","))
									finalValues = append(finalValues, finalValue)
									querySize += len(finalValue) + 1
									if querySize > 100000 {
										batchIdx += idx
										break
									}
								}
								batchIdx = -1
								if err := c.execWithLogging(ctx,
									baseQuery+strings.Join(finalValues, ","),
								); err != nil {
									return err
								}
							}
						case queryTypeUpdate:
							return errors.New("UPDATE does not support batching")
						case queryTypeDelete:
							return errors.New("DELETE does not support batching")
						}
						if currentType != queryTypeNone {
							shared.AtomicInt64Max(&lsns[workerId], batch[len(batch)-1].lsn)
						}
						batchTimeout = make(<-chan time.Time)
						batch = batch[:0]
					}
					currentType = queryTypeNone
					currentTable = ""
					return nil
				}
				for {
					c.logger.Info("top select")
					select {
					case q, ok := <-queries[workerId]:
						if !ok {
							return finishBatch()
						}
						if currentType != q.ty || currentTable != q.table {
							if err := finishBatch(); err != nil {
								return err
							}
						}
						switch q.ty {
						case queryTypeUpdate:
							assign := make([]string, 0, len(q.columns))
							where := make([]string, 0, len(q.whereColumns))
							for idx, colName := range q.columns {
								assign = append(assign, fmt.Sprintf("%s=%s",
									peerdb_clickhouse.QuoteIdentifier(colName),
									q.values[idx],
								))
							}
							for idx, colName := range q.whereColumns {
								where = append(where, fmt.Sprintf("%s=%s",
									peerdb_clickhouse.QuoteIdentifier(colName),
									q.whereValues[idx],
								))
							}
							if err := c.execWithLogging(ctx, fmt.Sprintf("UPDATE %s SET %s WHERE %s",
								peerdb_clickhouse.QuoteIdentifier(q.table),
								strings.Join(assign, ","),
								strings.Join(where, " AND "),
							)); err != nil {
								return err
							}
							shared.AtomicInt64Max(&lsns[workerId], q.lsn)
						case queryTypeDelete:
							where := make([]string, 0, len(q.whereColumns))
							for idx, colName := range q.whereColumns {
								where = append(where, fmt.Sprintf("%s=%s",
									peerdb_clickhouse.QuoteIdentifier(colName),
									q.whereValues[idx],
								))
							}
							if err := c.execWithLogging(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s",
								peerdb_clickhouse.QuoteIdentifier(q.table),
								strings.Join(where, " AND "),
							)); err != nil {
								return err
							}
							shared.AtomicInt64Max(&lsns[workerId], q.lsn)
						case queryTypeInsert:
							if len(batch) == 0 {
								batchTimeout = time.After(time.Second)
							}
							batch = append(batch, q)
							currentType = q.ty
							currentTable = q.table
						}
					case <-batchTimeout:
						if err := finishBatch(); err != nil {
							return err
						}
					case <-groupCtx.Done():
						return nil
					}
				}
			})
		}
	Loop:
		for {
			select {
			case record, ok := <-req.Records.GetRecords():
				if !ok {
					c.logger.Info("flushing batches because no more records")
					break Loop
				}
				numRecords += 1
				tableSchema, ok := req.TableNameSchemaMapping[record.GetDestinationTableName()]
				if !ok {
					c.logger.Warn("missing schema for table, ignoring", slog.String("table", record.GetDestinationTableName()))
					continue
				}
				fnvHash.Reset()
				orderingKeySlice := make([]string, 0)
				for _, tm := range req.TableMappings {
					if tm.DestinationTableIdentifier == record.GetDestinationTableName() && tm.SourceTableIdentifier == record.GetSourceTableName() {
						for _, col := range tm.Columns {
							if col.Ordering > 0 {
								orderingKeySlice = append(orderingKeySlice, col.DestinationName)
							}
						}
						break
					}
				}
				if len(orderingKeySlice) == 0 {
					orderingKeySlice = tableSchema.PrimaryKeyColumns
				}
				orderingKey := make(map[string]struct{}, len(orderingKeySlice))
				for _, colName := range orderingKeySlice {
					orderingKey[colName] = struct{}{}
				}
				lsn := record.GetBaseRecord().CheckpointID
				switch r := record.(type) {
				case *model.InsertRecord[model.RecordItems]:
					colNames := make([]string, 0, len(tableSchema.Columns))
					values := make([]string, 0, len(tableSchema.Columns))
					formattedValMap := make(map[string]string, len(orderingKeySlice))
					for _, col := range tableSchema.Columns {
						colNames = append(colNames, col.Name)
						val := r.Items.GetColumnValue(col.Name)
						formattedVal := formatQValue(val, tableSchema.NullableEnabled && col.Nullable)
						values = append(values, formattedVal)
						if _, isOrdering := orderingKey[col.Name]; isOrdering {
							formattedValMap[col.Name] = formattedVal
						}
					}
					for _, colName := range orderingKeySlice {
						io.WriteString(fnvHash, formattedValMap[colName])
					}
					queries[fnvHash.Sum64()%uint64(len(queries))] <- query{
						ty:      queryTypeInsert,
						table:   r.DestinationTableName,
						columns: colNames,
						values:  values,
						lsn:     lsn,
					}
				case *model.UpdateRecord[model.RecordItems]:
					columns := make([]string, 0, len(tableSchema.Columns))
					values := make([]string, 0, len(tableSchema.Columns))
					for _, col := range tableSchema.Columns {
						if _, unchanged := r.UnchangedToastColumns[col.Name]; unchanged {
							continue
						}
						if _, isOrdering := orderingKey[col.Name]; isOrdering {
							// CH does not support UPDATE on these columns
							continue
						}
						columns = append(columns, col.Name)
						values = append(values, formatQValue(r.NewItems.GetColumnValue(col.Name), tableSchema.NullableEnabled && col.Nullable))
					}
					whereValues := make([]string, 0, len(orderingKeySlice))
					for _, colName := range orderingKeySlice {
						item := r.OldItems.GetColumnValue(colName)
						if item == nil {
							item = r.NewItems.GetColumnValue(colName)
						}
						var nullable bool // TODO be a map lookup
						if tableSchema.NullableEnabled {
							for _, col := range tableSchema.Columns {
								if col.Name == colName {
									if col.Nullable {
										nullable = true
									}
									break
								}
							}
						}
						formattedVal := formatQValue(item, nullable)
						whereValues = append(whereValues, formattedVal)
						if _, isOrdering := orderingKey[colName]; isOrdering {
							io.WriteString(fnvHash, formattedVal)
						}
					}
					queries[fnvHash.Sum64()%uint64(len(queries))] <- query{
						ty:           queryTypeUpdate,
						table:        r.DestinationTableName,
						values:       values,
						columns:      columns,
						whereValues:  whereValues,
						whereColumns: orderingKeySlice,
						lsn:          lsn,
					}
				case *model.DeleteRecord[model.RecordItems]:
					values := make([]string, 0, len(orderingKeySlice))
					for _, colName := range orderingKeySlice {
						var nullable bool // TODO be a map lookup
						if tableSchema.NullableEnabled {
							for _, col := range tableSchema.Columns {
								if col.Name == colName {
									if col.Nullable {
										nullable = true
									}
									break
								}
							}
						}
						formattedVal := formatQValue(r.Items.GetColumnValue(colName), nullable)
						values = append(values, formattedVal)
						if _, isOrdering := orderingKey[colName]; isOrdering {
							io.WriteString(fnvHash, formattedVal)
						}
					}
					queries[fnvHash.Sum64()%uint64(len(queries))] <- query{
						ty:           queryTypeDelete,
						table:        r.DestinationTableName,
						whereValues:  values,
						whereColumns: orderingKeySlice,
						lsn:          lsn,
					}
				}
			case <-groupCtx.Done():
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				c.logger.Error("error syncing, ending batch", slog.Any("error", groupCtx.Err()))
				break Loop
			}
		}

		for _, ch := range queries {
			close(ch)
		}
		if err := group.Wait(); err != nil {
			return nil, err
		}

		lastCheckpoint := req.Records.GetLastCheckpoint()
		if err := c.FinishBatch(ctx, req.FlowJobName, req.SyncBatchID, lastCheckpoint); err != nil {
			return nil, err
		}
		return &model.SyncResponse{
			CurrentSyncBatchID:   req.SyncBatchID,
			LastSyncedCheckpoint: lastCheckpoint,
			NumRecordsSynced:     numRecords,
			TableNameRowsMapping: tableNameRowsMapping,
			TableSchemaDeltas:    req.Records.SchemaDeltas,
		}, nil
	} else {
		res, err := c.syncRecordsViaAvro(ctx, req, req.SyncBatchID)
		if err != nil {
			return nil, err
		}

		if err := c.FinishBatch(ctx, req.FlowJobName, req.SyncBatchID, res.LastSyncedCheckpoint); err != nil {
			c.logger.Error("failed to increment id", slog.Any("error", err))
			return nil, err
		}

		return res, nil
	}
}

func (c *ClickHouseConnector) ReplayTableSchemaDeltas(
	ctx context.Context,
	env map[string]string,
	flowJobName string,
	tableMappings []*protos.TableMapping,
	schemaDeltas []*protos.TableSchemaDelta,
) error {
	if len(schemaDeltas) == 0 {
		return nil
	}

	onCluster := c.onCluster()
	for _, schemaDelta := range schemaDeltas {
		if schemaDelta == nil || len(schemaDelta.AddedColumns) == 0 {
			continue
		}

		var tm *protos.TableMapping
		for _, tableMapping := range tableMappings {
			if tableMapping.SourceTableIdentifier == schemaDelta.SrcTableName &&
				tableMapping.DestinationTableIdentifier == schemaDelta.DstTableName {
				tm = tableMapping
				break
			}
		}

		for _, addedColumn := range schemaDelta.AddedColumns {
			qvKind := types.QValueKind(addedColumn.Type)
			clickHouseColType, err := qvalue.ToDWHColumnType(
				ctx, qvKind, env, protos.DBType_CLICKHOUSE, c.chVersion, addedColumn, schemaDelta.NullableEnabled,
			)
			if err != nil {
				return fmt.Errorf("failed to convert column type %s to ClickHouse type: %w", addedColumn.Type, err)
			}

			// Distributed table isn't created for null tables, no need to alter shard tables that don't exist
			if c.Config.Cluster != "" && (tm == nil || tm.Engine != protos.TableEngine_CH_ENGINE_NULL) {
				if err := c.execWithLogging(ctx,
					fmt.Sprintf("ALTER TABLE %s%s ADD COLUMN IF NOT EXISTS %s %s",
						peerdb_clickhouse.QuoteIdentifier(schemaDelta.DstTableName+"_shard"), onCluster,
						peerdb_clickhouse.QuoteIdentifier(addedColumn.Name), clickHouseColType),
				); err != nil {
					return fmt.Errorf("failed to add column %s for table shards %s: %w", addedColumn.Name, schemaDelta.DstTableName, err)
				}
			}

			if err := c.execWithLogging(ctx,
				fmt.Sprintf("ALTER TABLE %s%s ADD COLUMN IF NOT EXISTS %s %s",
					peerdb_clickhouse.QuoteIdentifier(schemaDelta.DstTableName), onCluster,
					peerdb_clickhouse.QuoteIdentifier(addedColumn.Name), clickHouseColType),
			); err != nil {
				return fmt.Errorf("failed to add column %s for table %s: %w", addedColumn.Name, schemaDelta.DstTableName, err)
			}
			c.logger.Info(
				"[schema delta replay] added column",
				slog.String("column", addedColumn.Name), slog.String("type", clickHouseColType),
				slog.String("destination table name", schemaDelta.DstTableName), slog.String("source table name", schemaDelta.SrcTableName),
			)
		}
	}

	return nil
}

func (c *ClickHouseConnector) RenameTables(
	ctx context.Context,
	req *protos.RenameTablesInput,
	tableNameSchemaMapping map[string]*protos.TableSchema,
) (*protos.RenameTablesOutput, error) {
	onCluster := c.onCluster()
	for _, renameRequest := range req.RenameTableOptions {
		if renameRequest.CurrentName == renameRequest.NewName {
			c.logger.Info("table rename is nop, probably Null table engine, skipping rename for it",
				slog.String("table", renameRequest.CurrentName))
			continue
		}

		resyncTableExists, err := c.checkIfTableExists(ctx, c.Config.Database, renameRequest.CurrentName)
		if err != nil {
			return nil, fmt.Errorf("unable to check if resync table %s exists: %w", renameRequest.CurrentName, err)
		}

		if !resyncTableExists {
			c.logger.Info("table does not exist, skipping rename for it", slog.String("table", renameRequest.CurrentName))
			continue
		}

		originalTableExists, err := c.checkIfTableExists(ctx, c.Config.Database, renameRequest.NewName)
		if err != nil {
			return nil, fmt.Errorf("unable to check if table %s exists: %w", renameRequest.NewName, err)
		}

		if originalTableExists {
			// target table exists, so we can attempt to swap. In most cases, we will have Atomic engine,
			// which supports a special query to exchange two tables, allowing dependent (materialized) views and dictionaries on these tables
			c.logger.Info("attempting atomic exchange",
				slog.String("OldName", renameRequest.CurrentName), slog.String("NewName", renameRequest.NewName))
			if err = c.execWithLogging(ctx,
				fmt.Sprintf("EXCHANGE TABLES %s and %s%s", peerdb_clickhouse.QuoteIdentifier(renameRequest.NewName),
					peerdb_clickhouse.QuoteIdentifier(renameRequest.CurrentName), onCluster),
			); err == nil {
				if err := c.execWithLogging(ctx,
					fmt.Sprintf(dropTableIfExistsSQL, peerdb_clickhouse.QuoteIdentifier(renameRequest.CurrentName), onCluster),
				); err != nil {
					return nil, fmt.Errorf("unable to drop exchanged table %s: %w", renameRequest.CurrentName, err)
				}
			} else if ex, ok := err.(*clickhouse.Exception); !ok || chproto.Error(ex.Code) != chproto.ErrNotImplemented {
				// move on to the fallback code if unimplemented, in all other error codes / types return,
				// since we know/assume exchange would be the sensible action
				return nil, fmt.Errorf("unable to exchange tables %s and %s: %w", renameRequest.NewName, renameRequest.CurrentName, err)
			}
		}

		// either original table doesn't exist, in which case it is safe to just run rename,
		// or err is set (in which case err comes from EXCHANGE TABLES)
		if !originalTableExists || err != nil {
			if err := c.execWithLogging(ctx,
				fmt.Sprintf(dropTableIfExistsSQL, peerdb_clickhouse.QuoteIdentifier(renameRequest.NewName), onCluster),
			); err != nil {
				return nil, fmt.Errorf("unable to drop table %s: %w", renameRequest.NewName, err)
			}

			if err := c.execWithLogging(ctx, fmt.Sprintf("RENAME TABLE %s TO %s%s",
				peerdb_clickhouse.QuoteIdentifier(renameRequest.CurrentName),
				peerdb_clickhouse.QuoteIdentifier(renameRequest.NewName), onCluster,
			)); err != nil {
				return nil, fmt.Errorf("unable to rename table %s to %s: %w", renameRequest.CurrentName, renameRequest.NewName, err)
			}
		}

		c.logger.Info("successfully renamed table",
			slog.String("OldName", renameRequest.CurrentName), slog.String("NewName", renameRequest.NewName))
	}

	return &protos.RenameTablesOutput{
		FlowJobName: req.FlowJobName,
	}, nil
}

func (c *ClickHouseConnector) SyncFlowCleanup(ctx context.Context, jobName string) error {
	// delete raw table if exists
	rawTableIdentifier := c.GetRawTableName(jobName)
	onCluster := c.onCluster()
	if err := c.execWithLogging(ctx,
		fmt.Sprintf(dropTableIfExistsSQL, peerdb_clickhouse.QuoteIdentifier(rawTableIdentifier), onCluster),
	); err != nil {
		return fmt.Errorf("[clickhouse] unable to drop raw table: %w", err)
	}
	if onCluster != "" {
		if err := c.execWithLogging(ctx,
			fmt.Sprintf(dropTableIfExistsSQL, peerdb_clickhouse.QuoteIdentifier(rawTableIdentifier+"_shard"), onCluster),
		); err != nil {
			return fmt.Errorf("[clickhouse] unable to drop raw table: %w", err)
		}
	}
	c.logger.Info("successfully dropped raw table", slog.String("table", rawTableIdentifier))

	return nil
}

func (c *ClickHouseConnector) RemoveTableEntriesFromRawTable(
	ctx context.Context,
	req *protos.RemoveTablesFromRawTableInput,
) error {
	if c.Config.Cluster != "" {
		// this operation isn't crucial, okay to skip
		c.logger.Info("skipping raw table cleanup of tables, DELETE not supported on Distributed table engine",
			slog.Any("tables", req.DestinationTableNames))
		return nil
	}

	for _, tableName := range req.DestinationTableNames {
		// Better to use lightweight deletes here as the main goal is to not have
		// rows in the table be visible by the NormalizeRecords' INSERT INTO SELECT queries
		if err := c.execWithLogging(ctx, fmt.Sprintf("DELETE FROM %s WHERE _peerdb_destination_table_name = %s"+
			" AND _peerdb_batch_id > %d AND _peerdb_batch_id <= %d",
			c.GetRawTableName(req.FlowJobName), peerdb_clickhouse.QuoteLiteral(tableName), req.NormalizeBatchId, req.SyncBatchId),
		); err != nil {
			return fmt.Errorf("unable to remove table %s from raw table: %w", tableName, err)
		}

		c.logger.Info("successfully removed entries for table from raw table", slog.String("table", tableName))
	}

	return nil
}
