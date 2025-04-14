package connpostgres

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type RecordStreamSink struct {
	*model.QRecordStream
}

func (stream RecordStreamSink) ExecuteQueryWithTx(
	ctx context.Context,
	qe *QRepQueryExecutor,
	tx pgx.Tx,
	query string,
	args ...any,
) (int64, int64, error) {
	defer shared.RollbackTx(tx, qe.logger)

	if qe.snapshot != "" {
		if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT "+utils.QuoteLiteral(qe.snapshot)); err != nil {
			qe.logger.Error("[pg_query_executor] failed to set snapshot",
				slog.Any("error", err), slog.String("query", query))
			return 0, 0, fmt.Errorf("[pg_query_executor] failed to set snapshot: %w", err)
		}
	}

	//nolint:gosec // number has no cryptographic significance
	randomUint := rand.Uint64()

	cursorName := fmt.Sprintf("peerdb_cursor_%d", randomUint)
	fetchSize := shared.FetchAndChannelSize
	cursorQuery := fmt.Sprintf("DECLARE %s CURSOR FOR %s", cursorName, query)

	if _, err := tx.Exec(ctx, cursorQuery, args...); err != nil {
		qe.logger.Info("[pg_query_executor] failed to declare cursor",
			slog.String("cursorQuery", cursorQuery), slog.Any("args", args), slog.Any("error", err))
		return 0, 0, fmt.Errorf("[pg_query_executor] failed to declare cursor: %w", err)
	} else {
		qe.logger.Info("[pg_query_executor] declared cursor", slog.String("cursorQuery", cursorQuery), slog.Any("args", args))
	}

	var totalNumRows int64
	var totalNumBytes int64
	for {
		numRows, numBytes, err := qe.processFetchedRows(ctx, query, tx, cursorName, fetchSize, stream.QRecordStream)
		if err != nil {
			qe.logger.Error("[pg_query_executor] failed to process fetched rows", slog.Any("error", err))
			return totalNumRows, totalNumBytes, err
		}

		qe.logger.Info("[pg_query_executor] fetched rows", slog.Int64("rows", numRows), slog.String("query", query))
		totalNumRows += numRows
		totalNumBytes += numBytes

		if numRows == 0 {
			break
		}
	}

	qe.logger.Info("Committing transaction")
	if err := tx.Commit(ctx); err != nil {
		qe.logger.Error("[pg_query_executor] failed to commit transaction", slog.Any("error", err))
		return totalNumRows, totalNumBytes, fmt.Errorf("[pg_query_executor] failed to commit transaction: %w", err)
	}

	qe.logger.Info("[pg_query_executor] committed transaction for query",
		slog.String("query", query), slog.Int64("rows", totalNumRows), slog.Int64("bytes", totalNumBytes))
	return totalNumRows, totalNumBytes, nil
}

func (stream RecordStreamSink) CopyInto(ctx context.Context, _ *PostgresConnector, tx pgx.Tx, table pgx.Identifier) (int64, error) {
	columnNames, err := stream.GetColumnNames()
	if err != nil {
		return 0, err
	}
	return tx.CopyFrom(ctx, table, columnNames, model.NewQRecordCopyFromSource(stream.QRecordStream))
}

func (stream RecordStreamSink) GetColumnNames() ([]string, error) {
	schema, err := stream.Schema()
	if err != nil {
		return nil, err
	}
	return schema.GetColumnNames(), nil
}
