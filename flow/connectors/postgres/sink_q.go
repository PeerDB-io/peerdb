package connpostgres

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
)

type RecordStreamSink struct {
	*model.QRecordStream
}

func (stream RecordStreamSink) ExecuteQueryWithTx(
	ctx context.Context,
	qe *QRepQueryExecutor,
	tx pgx.Tx,
	query string,
	args ...interface{},
) (int, error) {
	defer shared.RollbackTx(tx, qe.logger)

	if qe.snapshot != "" {
		_, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT "+QuoteLiteral(qe.snapshot))
		if err != nil {
			qe.logger.Error("[pg_query_executor] failed to set snapshot",
				slog.Any("error", err), slog.String("query", query))
			err := fmt.Errorf("[pg_query_executor] failed to set snapshot: %w", err)
			stream.Close(err)
			return 0, err
		}
	}

	//nolint:gosec // number has no cryptographic significance
	randomUint := rand.Uint64()

	cursorName := fmt.Sprintf("peerdb_cursor_%d", randomUint)
	fetchSize := shared.FetchAndChannelSize
	cursorQuery := fmt.Sprintf("DECLARE %s CURSOR FOR %s", cursorName, query)
	qe.logger.Info(fmt.Sprintf("[pg_query_executor] executing cursor declaration for %v with args %v", cursorQuery, args))

	if _, err := tx.Exec(ctx, cursorQuery, args...); err != nil {
		qe.logger.Info("[pg_query_executor] failed to declare cursor",
			slog.String("cursorQuery", cursorQuery), slog.Any("error", err))
		err = fmt.Errorf("[pg_query_executor] failed to declare cursor: %w", err)
		stream.Close(err)
		return 0, err
	}

	qe.logger.Info(fmt.Sprintf("[pg_query_executor] declared cursor '%s' for query '%s'", cursorName, query))

	totalRecordsFetched := 0
	for {
		numRows, err := qe.processFetchedRows(ctx, query, tx, cursorName, fetchSize, stream.QRecordStream)
		if err != nil {
			qe.logger.Error("[pg_query_executor] failed to process fetched rows", slog.Any("error", err))
			return 0, err
		}

		qe.logger.Info(fmt.Sprintf("[pg_query_executor] fetched %d rows for query '%s'", numRows, query))
		totalRecordsFetched += numRows

		if numRows == 0 {
			break
		}
	}

	qe.logger.Info("Committing transaction")
	if err := tx.Commit(ctx); err != nil {
		qe.logger.Error("[pg_query_executor] failed to commit transaction", slog.Any("error", err))
		err = fmt.Errorf("[pg_query_executor] failed to commit transaction: %w", err)
		stream.Close(err)
		return 0, err
	}

	qe.logger.Info(fmt.Sprintf("[pg_query_executor] committed transaction for query '%s', rows = %d",
		query, totalRecordsFetched))
	return totalRecordsFetched, nil
}

func (stream RecordStreamSink) CopyInto(ctx context.Context, _ *PostgresConnector, tx pgx.Tx, table pgx.Identifier) (int64, error) {
	return tx.CopyFrom(ctx, table, stream.GetColumnNames(), model.NewQRecordCopyFromSource(stream.QRecordStream))
}

func (stream RecordStreamSink) GetColumnNames() []string {
	return stream.Schema().GetColumnNames()
}
