package conncockroachdb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// Protected timestamps for the initial snapshot: SetupReplication best-effort
// pins MVCC history at the snapshot timestamp through
// crdb_internal.protect_mvcc_history (CockroachDB 24.1+) so a long initial
// load cannot lose its AS OF SYSTEM TIME reads to garbage collection,
// independent of gc.ttlseconds. The protection is a HISTORY RETENTION job
// that expires on its own after historyRetentionWindow; the changefeed side
// cancels it as soon as a session starts, with PullFlowCleanup as backstop.

const (
	// historyRetentionWindow bounds how long the snapshot timestamp stays
	// protected if the mirror never reaches the changefeed stage: expiry is
	// the leak guard, not the intended release path.
	historyRetentionWindow = 24 * time.Hour

	historyRetentionDescriptionPrefix = "peerdb initial load "
)

func historyRetentionDescription(flowJobName string) string {
	return historyRetentionDescriptionPrefix + flowJobName
}

func shouldProtectSnapshotHistory(config *protos.CockroachDBConfig, doInitialSnapshot bool) bool {
	return doInitialSnapshot && config.UseChangefeeds
}

func protectMVCCHistoryQuery(window time.Duration) string {
	return fmt.Sprintf("SELECT crdb_internal.protect_mvcc_history($1::decimal, '%d seconds'::interval, $2)",
		int64(window.Seconds()))
}

func (c *CockroachDBConnector) protectSnapshotHistory(ctx context.Context, flowJobName string, systemTime string) {
	// dedicated session so allow_unsafe_internals does not linger on the shared connection
	connConfig, err := ParseConfig(c.connStr, c.Config)
	if err != nil {
		c.logger.Warn("[cockroachdb] unable to protect snapshot timestamp", slog.Any("error", err))
		return
	}
	conn, err := NewCockroachDBConnFromConfig(ctx, connConfig, c.ssh)
	if err != nil {
		c.logger.Warn("[cockroachdb] unable to protect snapshot timestamp", slog.Any("error", err))
		return
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			c.logger.Debug("[cockroachdb] failed to close history protection connection", slog.Any("error", err))
		}
	}()

	// v26.1+ gates crdb_internal builtins behind this session variable; 25.x
	// has it defaulting to allow and older versions do not know it at all
	if _, err := conn.Exec(ctx, "SET allow_unsafe_internals = on"); err != nil {
		c.logger.Debug("[cockroachdb] could not set allow_unsafe_internals", slog.Any("error", err))
	}

	var jobID int64
	if err := conn.QueryRow(ctx, protectMVCCHistoryQuery(historyRetentionWindow),
		systemTime, historyRetentionDescription(flowJobName)).Scan(&jobID); err != nil {
		c.logger.Warn("[cockroachdb] could not protect the snapshot timestamp from garbage collection,"+
			" the initial load relies on gc.ttlseconds alone; protection requires CockroachDB 24.1+,"+
			" the REPLICATION privilege, and on 26.1+ allow_unsafe_internals = on"+
			" (or the sql.override.allow_unsafe_internals.enabled cluster setting)",
			slog.String("systemTime", systemTime), slog.Any("error", err))
		return
	}
	c.logger.Info("[cockroachdb] protected snapshot timestamp from garbage collection",
		slog.String("systemTime", systemTime),
		slog.Int64("jobID", jobID),
		slog.Duration("expiresAfter", historyRetentionWindow))
}

// releaseSnapshotHistoryProtection cancels the flow's HISTORY RETENTION job
// if one is still running. The job is found by its description, so nothing
// has to be persisted and repeated calls are safe no-ops.
func (c *CockroachDBConnector) releaseSnapshotHistoryProtection(ctx context.Context, flowJobName string) {
	var jobID int64
	if err := c.conn.QueryRow(ctx,
		"SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'HISTORY RETENTION'"+
			" AND status = 'running' AND description LIKE '%' || $1 LIMIT 1",
		historyRetentionDescription(flowJobName)).Scan(&jobID); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			c.logger.Warn("[cockroachdb] failed to look up snapshot history retention job", slog.Any("error", err))
		}
		return
	}
	if _, err := c.conn.Exec(ctx, "CANCEL JOB $1", jobID); err != nil {
		c.logger.Warn("[cockroachdb] failed to cancel snapshot history retention job",
			slog.Int64("jobID", jobID), slog.Any("error", err))
		return
	}
	c.logger.Info("[cockroachdb] released snapshot history retention job", slog.Int64("jobID", jobID))
}
