package connpostgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/datatypes"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type QRepQueryExecutor struct {
	*PostgresConnector
	logger      log.Logger
	snapshot    string
	flowJobName string
	partitionID string
}

func (c *PostgresConnector) NewQRepQueryExecutor(ctx context.Context,
	flowJobName string, partitionID string,
) (*QRepQueryExecutor, error) {
	return c.NewQRepQueryExecutorSnapshot(ctx, "", flowJobName, partitionID)
}

func (c *PostgresConnector) NewQRepQueryExecutorSnapshot(ctx context.Context,
	snapshot string, flowJobName string, partitionID string,
) (*QRepQueryExecutor, error) {
	_, err := c.fetchCustomTypeMapping(ctx)
	if err != nil {
		c.logger.Error("[pg_query_executor] failed to fetch custom type mapping", slog.Any("error", err))
		return nil, fmt.Errorf("failed to fetch custom type mapping: %w", err)
	}
	return &QRepQueryExecutor{
		PostgresConnector: c,
		snapshot:          snapshot,
		flowJobName:       flowJobName,
		partitionID:       partitionID,
		logger:            log.With(c.logger, slog.String(string(shared.PartitionIDKey), partitionID)),
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

func (qe *QRepQueryExecutor) executeQueryInTx(ctx context.Context, tx pgx.Tx, cursorName string, fetchSize int) (pgx.Rows, error) {
	qe.logger.Info("Executing query in transaction")
	q := fmt.Sprintf("FETCH %d FROM %s", fetchSize, cursorName)

	rows, err := tx.Query(ctx, q)
	if err != nil {
		qe.logger.Error("[pg_query_executor] failed to execute query in tx", slog.Any("error", err))
		return nil, err
	}

	return rows, nil
}

// FieldDescriptionsToSchema converts a slice of pgconn.FieldDescription to a QRecordSchema.
func (qe *QRepQueryExecutor) fieldDescriptionsToSchema(fds []pgconn.FieldDescription) qvalue.QRecordSchema {
	qfields := make([]qvalue.QField, len(fds))
	for i, fd := range fds {
		ctype := qe.postgresOIDToQValueKind(fd.DataTypeOID, qe.customTypeMapping)
		// there isn't a way to know if a column is nullable or not
		if ctype == qvalue.QValueKindNumeric {
			precision, scale := datatypes.ParseNumericTypmod(fd.TypeModifier)
			qfields[i] = qvalue.QField{
				Name:      fd.Name,
				Type:      ctype,
				Nullable:  true,
				Precision: precision,
				Scale:     scale,
			}
		} else {
			qfields[i] = qvalue.QField{
				Name:     fd.Name,
				Type:     ctype,
				Nullable: true,
			}
		}
	}
	return qvalue.NewQRecordSchema(qfields)
}

func (qe *QRepQueryExecutor) processRowsStream(
	ctx context.Context,
	cursorName string,
	stream *model.QRecordStream,
	rows pgx.Rows,
	fieldDescriptions []pgconn.FieldDescription,
) (int64, int64, error) {
	var numRows int64
	var numBytes int64
	const logPerRows = 10000

	for rows.Next() {
		if err := ctx.Err(); err != nil {
			qe.logger.Info("Context canceled, exiting processRowsStream early")
			return numRows, numBytes, err
		}

		record, err := qe.mapRowToQRecord(rows, fieldDescriptions)
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
			qe.logger.Info("processing row stream", slog.String("cursor", cursorName),
				slog.Int64("records", numRows), slog.Int64("bytes", numBytes))
		}
	}

	qe.logger.Info("processed row stream", slog.String("cursor", cursorName), slog.Int64("records", numRows), slog.Int64("bytes", numBytes))
	return numRows, numBytes, nil
}

func (qe *QRepQueryExecutor) processFetchedRows(
	ctx context.Context,
	query string,
	tx pgx.Tx,
	cursorName string,
	fetchSize int,
	stream *model.QRecordStream,
) (int64, int64, error) {
	rows, err := qe.executeQueryInTx(ctx, tx, cursorName, fetchSize)
	if err != nil {
		qe.logger.Error("[pg_query_executor] failed to execute query in tx",
			slog.Any("error", err), slog.String("query", query))
		return 0, 0, fmt.Errorf("[pg_query_executor] failed to execute query in tx: %w", err)
	}
	defer rows.Close()

	fieldDescriptions := rows.FieldDescriptions()
	if !stream.IsSchemaSet() {
		schema := qe.fieldDescriptionsToSchema(fieldDescriptions)
		stream.SetSchema(schema)
	}

	numRows, numBytes, err := qe.processRowsStream(ctx, cursorName, stream, rows, fieldDescriptions)
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
		if _, _, err := qe.ExecuteAndProcessQueryStream(ctx, stream, query, args...); err != nil {
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
	query string,
	args ...any,
) (int64, int64, error) {
	return qe.ExecuteQueryIntoSink(
		ctx,
		RecordStreamSink{QRecordStream: stream},
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
	fds []pgconn.FieldDescription,
) ([]qvalue.QValue, error) {
	// make vals an empty array of QValue of size len(fds)
	record := make([]qvalue.QValue, len(fds))

	values, err := row.Values()
	if err != nil {
		qe.logger.Error("[pg_query_executor] failed to get values from row", slog.Any("error", err))
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	for i, fd := range fds {
		tmp, err := qe.parseFieldFromPostgresOID(fd.DataTypeOID, values[i], qe.customTypeMapping)
		if err != nil {
			qe.logger.Error("[pg_query_executor] failed to parse field", slog.Any("error", err))
			return nil, fmt.Errorf("failed to parse field: %w", err)
		}
		record[i] = tmp
	}

	return record, nil
}
