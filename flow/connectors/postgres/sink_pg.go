package connpostgres

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"go.temporal.io/sdk/temporal"

	"github.com/PeerDB-io/peerdb/flow/connectors/postgres/sanitize"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/concurrency"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

type PgCopyShared struct {
	schemaLatch *concurrency.Latch[[]string]
	err         error
}

type PgCopyWriter struct {
	*io.PipeWriter
	schema *PgCopyShared
}

type PgCopyReader struct {
	*io.PipeReader
	schema *PgCopyShared
}

func NewPgCopyPipe() (PgCopyReader, PgCopyWriter) {
	read, write := io.Pipe()
	schema := PgCopyShared{schemaLatch: concurrency.NewLatch[[]string]()}
	return PgCopyReader{PipeReader: read, schema: &schema},
		PgCopyWriter{PipeWriter: write, schema: &schema}
}

func (p PgCopyWriter) SetSchema(schema []string) {
	p.schema.schemaLatch.Set(schema)
}

func (p PgCopyWriter) HandleQRepSyncError(err error) {
	p.Close(err)
}

func (p PgCopyWriter) ExecuteQueryWithTx(
	ctx context.Context,
	qe *QRepQueryExecutor,
	tx pgx.Tx,
	query string,
	args ...any,
) (int64, int64, error) {
	defer qe.Conn().Close(context.Background())
	defer shared.RollbackTx(tx, qe.logger)

	// Clear any existing deadline at the start to ensure clean state
	clearConnectionDeadline(qe.conn.PgConn(), qe.logger, "sink_pg start")

	// Clear any deadline set during execution to ensure commit/rollback can proceed
	// Must happen regardless of function exit path, so use defer
	defer clearConnectionDeadline(qe.conn.PgConn(), qe.logger, "sink_pg cleanup")

	if qe.snapshot != "" {
		// Use context.Background() to prevent ContextWatcher creation
		if _, err := tx.Exec(context.Background(), "SET TRANSACTION SNAPSHOT "+utils.QuoteLiteral(qe.snapshot)); err != nil {
			qe.logger.Error("[pg_query_executor] failed to set snapshot",
				slog.Any("error", err), slog.String("query", query))
			if shared.IsSQLStateError(err, pgerrcode.UndefinedObject, pgerrcode.InvalidParameterValue) {
				return 0, 0, temporal.NewNonRetryableApplicationError("failed to set transaction snapshot",
					exceptions.ApplicationErrorTypeIrrecoverableInvalidSnapshot.String(), err)
			} else if shared.IsSQLStateErrorSubstring(err,
				pgerrcode.ObjectNotInPrerequisiteState, "could not import the requested snapshot") {
				return 0, 0, temporal.NewNonRetryableApplicationError("failed to set transaction snapshot",
					exceptions.ApplicationErrorTypeIrrecoverableCouldNotImportSnapshot.String(), err)
			}
			return 0, 0, fmt.Errorf("[pg_query_executor] failed to set snapshot: %w", err)
		}
	}

	// Use context.Background() to prevent ContextWatcher creation
	norows, err := tx.Query(context.Background(), query+" limit 0", args...)
	if err != nil {
		return 0, 0, err
	}

	fieldDescriptions := norows.FieldDescriptions()
	cols := make([]string, 0, len(fieldDescriptions))
	for _, fd := range fieldDescriptions {
		cols = append(cols, fd.Name)
	}
	p.SetSchema(cols)
	norows.Close()

	query, err = sanitize.SanitizeSQL(query, args...)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to apply parameters to copy subquery: %w", err)
	}

	copyQuery := fmt.Sprintf("COPY (%s) TO STDOUT", query)
	qe.logger.Info("[pg_query_executor] executing copy", slog.String("query", copyQuery))

	// Monitor context cancellation and close pipe to trigger clean exit
	// Use context.Background() for CopyTo to avoid ContextWatcher entirely
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			p.PipeWriter.CloseWithError(ctx.Err())
		case <-done:
		}
	}()

	// Use Background context to prevent ContextWatcher creation (ctx == context.Background() check)
	// Cancellation is handled via pipe closing above, timeout is handled by Temporal activity timeout
	ct, err := qe.conn.PgConn().CopyTo(context.Background(), p.PipeWriter, copyQuery)
	if err != nil {
		// Close pipe explicitly to ensure destination side exits cleanly
		if closeErr := p.PipeWriter.CloseWithError(err); closeErr != nil {
			qe.logger.Warn("[pg_query_executor] failed to close pipe on copy error",
				slog.Any("closeError", closeErr), slog.Any("copyError", err))
		}
		qe.logger.Info("[pg_query_executor] failed to copy",
			slog.String("copyQuery", copyQuery), slog.Any("error", err))
		return 0, 0, fmt.Errorf("[pg_query_executor] failed to copy: %w", err)
	}

	qe.logger.Info("Committing transaction")
	commitCtx, commitCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer commitCancel()
	if err := tx.Commit(commitCtx); err != nil {
		qe.logger.Error("[pg_query_executor] failed to commit transaction", slog.Any("error", err))
		return 0, 0, fmt.Errorf("[pg_query_executor] failed to commit transaction: %w", err)
	}

	totalRecordsFetched := ct.RowsAffected()
	qe.logger.Info(fmt.Sprintf("[pg_query_executor] committed transaction for query '%s', rows = %d",
		query, totalRecordsFetched))
	return totalRecordsFetched, 0, nil
}

func (p PgCopyWriter) Close(err error) {
	p.PipeWriter.CloseWithError(err)
	p.schema.err = err
	p.SetSchema(nil)
}

func (p PgCopyReader) GetColumnNames() ([]string, error) {
	return p.schema.schemaLatch.Wait(), p.schema.err
}

func (p PgCopyReader) CopyInto(ctx context.Context, c *PostgresConnector, tx pgx.Tx, table pgx.Identifier) (int64, error) {
	cols, err := p.GetColumnNames()
	if err != nil {
		return 0, err
	}
	quotedCols := make([]string, 0, len(cols))
	for _, col := range cols {
		quotedCols = append(quotedCols, common.QuoteIdentifier(col))
	}

	// Monitor context cancellation and close pipe to trigger clean exit
	// Use context.Background() for CopyFrom to avoid ContextWatcher entirely
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			p.PipeReader.CloseWithError(ctx.Err())
		case <-done:
		}
	}()

	// Clear deadline immediately before CopyFrom as final safeguard against races in BeginTx
	// This handles the case where context was cancelled between BeginTx and here
	clearConnectionDeadline(tx.Conn().PgConn(), c.logger, "before CopyFrom")

	// Use Background context to prevent ContextWatcher creation (ctx == context.Background() check)
	// Cancellation is handled via pipe closing above, timeout is handled by Temporal activity timeout
	ct, err := tx.Conn().PgConn().CopyFrom(
		context.Background(),
		p.PipeReader,
		fmt.Sprintf("COPY %s (%s) FROM STDIN", table.Sanitize(), strings.Join(quotedCols, ",")),
	)
	return ct.RowsAffected(), err
}
