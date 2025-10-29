package connpostgres

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	jsoniter "github.com/json-iterator/go"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type QRepQueryExecutor struct {
	*PostgresConnector
	logger      log.Logger
	snapshot    string
	flowJobName string
	partitionID string
	version     uint32
}

func (c *PostgresConnector) NewQRepQueryExecutor(ctx context.Context, version uint32,
	flowJobName string, partitionID string,
) (*QRepQueryExecutor, error) {
	return c.NewQRepQueryExecutorSnapshot(ctx, version, "", flowJobName, partitionID)
}

func (c *PostgresConnector) NewQRepQueryExecutorSnapshot(ctx context.Context, version uint32,
	snapshot string, flowJobName string, partitionID string,
) (*QRepQueryExecutor, error) {
	if _, err := c.fetchCustomTypeMapping(ctx); err != nil {
		c.logger.Error("[pg_query_executor] failed to fetch custom type mapping", slog.Any("error", err))
		return nil, fmt.Errorf("failed to fetch custom type mapping: %w", err)
	}
	return &QRepQueryExecutor{
		PostgresConnector: c,
		snapshot:          snapshot,
		flowJobName:       flowJobName,
		partitionID:       partitionID,
		logger:            log.With(c.logger, slog.String(string(shared.PartitionIDKey), partitionID)),
		version:           version,
	}, nil
}

func (qe *QRepQueryExecutor) ExecuteQuery(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	rows, err := qe.conn.Query(ctx, query, args...)
	if err != nil {
		qe.logger.Error("[pg_query_executor] failed to execute query", slog.Any("error", err))
		return nil, err
	}
	return rows, nil
}

// FieldDescriptionsToSchema converts a slice of pgconn.FieldDescription to a QRecordSchema.
func (qe *QRepQueryExecutor) cursorToSchema(
	ctx context.Context,
	tx pgx.Tx,
	cursorName string,
) (types.QRecordSchema, error) {
	type attId struct {
		relid uint32
		num   uint16
	}

	rows, err := tx.Query(ctx, "FETCH 0 FROM "+cursorName)
	if err != nil {
		return types.QRecordSchema{}, fmt.Errorf("failed to fetch 0 for field descriptions: %w", err)
	}
	fds := rows.FieldDescriptions()
	tableOIDset := make(map[uint32]struct{})
	nullPointers := make(map[attId]*bool, len(fds))
	qfields := make([]types.QField, len(fds))
	for i, fd := range fds {
		tableOIDset[fd.TableOID] = struct{}{}
		ctype := qe.postgresOIDToQValueKind(fd.DataTypeOID, qe.customTypeMapping, qe.version)
		if ctype == types.QValueKindNumeric || ctype == types.QValueKindArrayNumeric {
			precision, scale := datatypes.ParseNumericTypmod(fd.TypeModifier)
			qfields[i] = types.QField{
				Name:      fd.Name,
				Type:      ctype,
				Nullable:  false,
				Precision: precision,
				Scale:     scale,
			}
		} else {
			qfields[i] = types.QField{
				Name:     fd.Name,
				Type:     ctype,
				Nullable: false,
			}
		}
		nullPointers[attId{
			relid: fd.TableOID,
			num:   fd.TableAttributeNumber,
		}] = &qfields[i].Nullable
	}
	rows.Close()
	tableOIDs := slices.Collect(maps.Keys(tableOIDset))

	rows, err = tx.Query(ctx, "SELECT a.attrelid,a.attnum FROM pg_attribute a WHERE a.attrelid = ANY($1) AND NOT a.attnotnull", tableOIDs)
	if err != nil {
		return types.QRecordSchema{}, fmt.Errorf("failed to query schema for field descriptions: %w", err)
	}

	var att attId
	if _, err := pgx.ForEachRow(rows, []any{&att.relid, &att.num}, func() error {
		if nullPointer, ok := nullPointers[att]; ok {
			*nullPointer = true
		}
		return nil
	}); err != nil {
		return types.QRecordSchema{}, fmt.Errorf("failed to process schema for field descriptions: %w", err)
	}

	return types.NewQRecordSchema(qfields), nil
}

func (qe *QRepQueryExecutor) processRowsStream(
	ctx context.Context,
	cursorName string,
	dstType protos.DBType,
	stream *model.QRecordStream,
	rows pgx.Rows,
	fieldDescriptions []pgconn.FieldDescription,
) (int64, int64, error) {
	var numRows int64
	var numBytes int64
	const logPerRows = 50000

	jsonApi := createExtendedJSONUnmarshaler()
	schema, err := stream.Schema()
	if err != nil {
		return 0, 0, err
	}
	nullableFields := make(map[string]struct{}, len(schema.Fields))
	for _, field := range schema.Fields {
		if field.Nullable {
			nullableFields[field.Name] = struct{}{}
		}
	}

	for rows.Next() {
		if err := ctx.Err(); err != nil {
			qe.logger.Info("Context canceled, exiting processRowsStream early")
			return numRows, numBytes, err
		}

		record, err := qe.mapRowToQRecord(rows, dstType, nullableFields, fieldDescriptions, jsonApi)
		if err != nil {
			qe.logger.Error("[pg_query_executor] failed to map row to QRecord", slog.Any("error", err))
			return numRows, numBytes, fmt.Errorf("failed to map row to QRecord: %w", err)
		}
		stream.Records <- record
		numRows++
		for _, val := range rows.RawValues() {
			numBytes += int64(len(val))
		}

		if numRows%logPerRows == 0 {
			qe.logger.Info("processing row stream",
				slog.String("cursor", cursorName),
				slog.Int64("records", numRows),
				slog.Int64("bytes", numBytes),
				slog.Int("channelLen", len(stream.Records)))
		}
	}

	qe.logger.Info("processed row stream",
		slog.String("cursor", cursorName),
		slog.Int64("records", numRows),
		slog.Int64("bytes", numBytes),
		slog.Int("channelLen", len(stream.Records)))
	return numRows, numBytes, nil
}

func (qe *QRepQueryExecutor) processFetchedRows(
	ctx context.Context,
	query string,
	tx pgx.Tx,
	cursorName string,
	fetchSize int,
	dstType protos.DBType,
	stream *model.QRecordStream,
) (int64, int64, error) {
	qe.logger.Info("[pg_query_executor] fetching from cursor", slog.String("cursor", cursorName))

	rows, err := tx.Query(ctx, fmt.Sprintf("FETCH %d FROM %s", fetchSize, cursorName))
	if err != nil {
		qe.logger.Error("[pg_query_executor] failed to fetch cursor in tx",
			slog.Any("error", err), slog.String("query", query))
		return 0, 0, fmt.Errorf("[pg_query_executor] failed to execute query in tx: %w", err)
	}
	defer rows.Close()

	fieldDescriptions := rows.FieldDescriptions()
	numRows, numBytes, err := qe.processRowsStream(ctx, cursorName, dstType, stream, rows, fieldDescriptions)
	if err != nil {
		qe.logger.Error("[pg_query_executor] failed to process rows", slog.Any("error", err))
		return numRows, numBytes, fmt.Errorf("failed to process rows: %w", err)
	}

	if err := rows.Err(); err != nil {
		qe.logger.Error("[pg_query_executor] row iteration failed",
			slog.String("query", query), slog.Any("error", err))
		return numRows, numBytes, fmt.Errorf("[pg_query_executor] row iteration failed '%s': %w", query, err)
	}

	return numRows, numBytes, nil
}

func (qe *QRepQueryExecutor) ExecuteAndProcessQuery(
	ctx context.Context,
	query string,
	args ...any,
) (*model.QRecordBatch, error) {
	stream := model.NewQRecordStream(1024)
	errors := make(chan struct{})
	var errorsError error
	qe.logger.Info("Executing and processing query", slog.String("query", query))

	// must wait on errors to close before returning to maintain qe.conn exclusion
	go func() {
		defer close(errors)
		if _, _, err := qe.ExecuteAndProcessQueryStream(ctx, stream, protos.DBType_DBTYPE_UNKNOWN, query, args...); err != nil {
			qe.logger.Error("[pg_query_executor] failed to execute and process query stream", slog.Any("error", err))
			errorsError = err
		}
	}()

	select {
	case <-errors:
		if errorsError == nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-stream.SchemaChan():
				schema, err := stream.Schema()
				if err != nil {
					return nil, err
				}
				return &model.QRecordBatch{
					Schema:  schema,
					Records: nil,
				}, nil
			}
		}
		return nil, errorsError
	case <-stream.SchemaChan():
		schema, err := stream.Schema()
		if err != nil {
			return nil, err
		}
		batch := &model.QRecordBatch{
			Schema:  schema,
			Records: nil,
		}
		for record := range stream.Records {
			batch.Records = append(batch.Records, record)
		}
		<-errors
		if errorsError != nil {
			return nil, errorsError
		}
		if err := stream.Err(); err != nil {
			return nil, fmt.Errorf("[pg] failed to get record from stream: %w", err)
		}
		return batch, nil
	}
}

func (qe *QRepQueryExecutor) ExecuteAndProcessQueryStream(
	ctx context.Context,
	stream *model.QRecordStream,
	dstType protos.DBType,
	query string,
	args ...any,
) (int64, int64, error) {
	return qe.ExecuteQueryIntoSink(
		ctx,
		RecordStreamSink{QRecordStream: stream, DestinationType: dstType},
		query,
		args...,
	)
}

func (qe *QRepQueryExecutor) ExecuteQueryIntoSink(
	ctx context.Context,
	sink QRepPullSink,
	query string,
	args ...any,
) (int64, int64, error) {
	qe.logger.Info("Executing and processing query stream", slog.String("query", query))
	defer sink.Close(nil)

	tx, err := qe.conn.BeginTx(ctx, pgx.TxOptions{
		AccessMode: pgx.ReadOnly,
		IsoLevel:   pgx.RepeatableRead,
	})
	if err != nil {
		qe.logger.Error("[pg_query_executor] failed to begin transaction", slog.Any("error", err))
		err := fmt.Errorf("[pg_query_executor] failed to begin transaction: %w", err)
		sink.Close(err)
		return 0, 0, err
	}

	totalRecords, totalBytes, err := sink.ExecuteQueryWithTx(ctx, qe, tx, query, args...)
	if err != nil {
		sink.Close(err)
	}
	return totalRecords, totalBytes, err
}

func (qe *QRepQueryExecutor) ExecuteQueryIntoSinkGettingCurrentSnapshotXmin(
	ctx context.Context,
	sink QRepPullSink,
	query string,
	args ...any,
) (int64, int64, int64, error) {
	var currentSnapshotXmin pgtype.Int8
	qe.logger.Info("Executing and processing query stream", slog.String("query", query))
	defer sink.Close(nil)

	tx, err := qe.conn.BeginTx(ctx, pgx.TxOptions{
		AccessMode: pgx.ReadOnly,
		IsoLevel:   pgx.RepeatableRead,
	})
	if err != nil {
		qe.logger.Error("[pg_query_executor] failed to begin transaction", slog.Any("error", err))
		err := fmt.Errorf("[pg_query_executor] failed to begin transaction: %w", err)
		sink.Close(err)
		return 0, 0, currentSnapshotXmin.Int64, err
	}

	if err := tx.QueryRow(ctx, "select txid_snapshot_xmin(txid_current_snapshot())").Scan(&currentSnapshotXmin); err != nil {
		qe.logger.Error("[pg_query_executor] failed to get current snapshot xmin", slog.Any("error", err))
		sink.Close(err)
		return 0, 0, currentSnapshotXmin.Int64, err
	}

	totalRecords, totalBytes, err := sink.ExecuteQueryWithTx(ctx, qe, tx, query, args...)
	if err != nil {
		sink.Close(err)
	}
	return totalRecords, totalBytes, currentSnapshotXmin.Int64, err
}

func (qe *QRepQueryExecutor) mapRowToQRecord(
	row pgx.Rows,
	dstType protos.DBType,
	nullableFields map[string]struct{},
	fds []pgconn.FieldDescription,
	jsonApi jsoniter.API,
) ([]types.QValue, error) {
	rawValues := row.RawValues()
	values := make([]any, len(fds))
	// Simulate the behavior of rows.Values() with a carveout for JSON
	for i, fd := range fds {
		buf := rawValues[i]
		if buf == nil {
			values[i] = nil
			continue
		}

		// Special handling for JSON types
		switch fd.DataTypeOID {
		case pgtype.JSONOID, pgtype.JSONBOID:
			if err := jsonApi.Unmarshal(buf, &values[i]); err != nil {
				qe.logger.Error("[pg_query_executor] failed to unmarshal json", slog.Any("error", err))
				return nil, fmt.Errorf("failed to unmarshal json: %w", err)
			}
			if values[i] == nil {
				// avoid confusing SQL null & JSON null by using pre-marshaled value
				values[i] = json.RawMessage("null")
			}
		case pgtype.JSONArrayOID, pgtype.JSONBArrayOID:
			var textArr pgtype.FlatArray[pgtype.Text]
			if err := qe.conn.TypeMap().Scan(fd.DataTypeOID, fd.Format, buf, &textArr); err != nil {
				qe.logger.Error("[pg_query_executor] failed to to scan json array", slog.Any("error", err))
				return nil, fmt.Errorf("failed to scan json array: %w", err)
			}

			arr := make([]any, len(textArr))
			for j, text := range textArr {
				if text.Valid {
					if err := jsonApi.UnmarshalFromString(text.String, &arr[j]); err != nil {
						qe.logger.Error("[pg_query_executor] failed to unmarshal json array element", slog.Any("error", err))
						return nil, fmt.Errorf("failed to unmarshal json array element: %w", err)
					}
				} else {
					arr[j] = nil
				}
			}
			values[i] = arr

		default:
			if dt, ok := qe.conn.TypeMap().TypeForOID(fd.DataTypeOID); ok {
				// Log numeric decoding to identify slow columns
				if fd.DataTypeOID == pgtype.NumericOID {
					bufText := base64.StdEncoding.EncodeToString(buf)
					qe.logger.Info("[pg_query_executor] about to decode numeric",
						slog.String("column", fd.Name),
						slog.Int("rawLen", len(buf)),
						slog.String("bufTextBase64", bufText),
					)
				}

				value, err := dt.Codec.DecodeValue(qe.conn.TypeMap(), fd.DataTypeOID, fd.Format, buf)

				if fd.DataTypeOID == pgtype.NumericOID {
					qe.logger.Info("[pg_query_executor] decoded numeric",
						slog.String("column", fd.Name))
				}

				if err != nil {
					qe.logger.Error("[pg_query_executor] failed to decode value", slog.Any("error", err))
					return nil, fmt.Errorf("failed to decode value: %w", err)
				}
				values[i] = value
			} else {
				// Unknown type - treat as text or binary based on format
				switch fd.Format {
				case pgtype.TextFormatCode:
					values[i] = string(buf)
				case pgtype.BinaryFormatCode:
					values[i] = slices.Clone(buf)
				default:
					qe.logger.Error("[pg_query_executor] unknown format code", slog.Int("format", int(fd.Format)))
					return nil, fmt.Errorf("unknown format code: %d", fd.Format)
				}
			}
		}
	}

	// Schema fields should generally align with field descriptors,
	// avoid building map until we detect they are misaligned
	record := make([]types.QValue, len(fds))
	for i, fd := range fds {
		_, nullable := nullableFields[fd.Name]
		tmp, err := qe.parseFieldFromPostgresOID(
			fd.DataTypeOID,
			fd.TypeModifier,
			nullable,
			dstType,
			values[i],
			qe.customTypeMapping,
			qe.version,
		)
		if err != nil {
			qe.logger.Error("[pg_query_executor] failed to parse field", slog.Any("error", err))
			return nil, fmt.Errorf("failed to parse field: %w", err)
		}
		record[i] = tmp
	}

	return record, nil
}
