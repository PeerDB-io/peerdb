package conncockroachdb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// Protected timestamps for the initial snapshot: SetupReplication best-effort
// pins MVCC history at the snapshot timestamp through
// crdb_internal.protect_mvcc_history (CockroachDB 24.1+) so a long initial
// load cannot lose its AS OF SYSTEM TIME reads to garbage collection,
// independent of gc.ttlseconds. The protection is a HISTORY RETENTION job
// that expires on its own after historyRetentionWindow unless the snapshot
// keeps extending it (extendSnapshotHistoryProtection); the changefeed side
// cancels it as soon as a session starts, with PullFlowCleanup as backstop.

const (
	// historyRetentionWindow bounds how long the snapshot timestamp stays
	// protected without a sign of life from the mirror: expiry is the orphan
	// guard, not the intended release path. Snapshots that outlive the window
	// stay protected anyway because every partition pull best-effort extends
	// the job (its expiry becomes last extension + window, see
	// extendSnapshotHistoryProtection), so the window only has to cover the
	// gap between SetupReplication and the first pull, one partition pull,
	// and the handoff from the last pull to the first changefeed session.
	historyRetentionWindow = 4 * time.Hour

	// historyRetentionExtendInterval throttles the per-partition extension:
	// at most one extension per interval per connector. A quarter of the
	// window keeps several extension chances within one window (small tables
	// finish inside the window without ever extending, huge ones refresh it
	// long before expiry) without a jobs-table write on every partition.
	historyRetentionExtendInterval = historyRetentionWindow / 4

	historyRetentionDescriptionPrefix = "peerdb initial load "

	// historyRetentionLookupQuery finds this flow's HISTORY RETENTION job by
	// its deterministic description. The exact suffix comparison (instead of a
	// LIKE pattern) keeps `_` and `%` in flow names from acting as wildcards
	// and matching another mirror's job. The ::text casts are load-bearing:
	// without them CockroachDB cannot infer the placeholder type inside
	// length() and fails with SQLSTATE 42P18.
	historyRetentionLookupQuery = "SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'HISTORY RETENTION'" +
		" AND status = 'running' AND right(description, length($1::text)) = $1::text LIMIT 1"

	// historyRetentionExtendQuery heartbeats a HISTORY RETENTION job:
	// crdb_internal.extend_mvcc_history_protection(job_id) returns void and
	// resets the job's expiry to now + its original expiration window, so
	// periodic calls keep the protection alive for as long as needed.
	historyRetentionExtendQuery = "SELECT crdb_internal.extend_mvcc_history_protection($1)"
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

// historyProtectionSession opens a dedicated connection for the crdb_internal
// history retention builtins so allow_unsafe_internals does not linger on the
// shared connection. v26.1+ gates crdb_internal builtins behind that session
// variable; 25.x has it defaulting to allow and older versions do not know it
// at all, so failing to set it only logs.
func (c *CockroachDBConnector) historyProtectionSession(ctx context.Context) (*pgx.Conn, error) {
	connConfig, err := ParseConfig(c.connStr, c.Config)
	if err != nil {
		return nil, err
	}
	conn, err := NewCockroachDBConnFromConfig(ctx, connConfig, c.ssh)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Exec(ctx, "SET allow_unsafe_internals = on"); err != nil {
		c.logger.Debug("[cockroachdb] could not set allow_unsafe_internals", slog.Any("error", err))
	}
	return conn, nil
}

func (c *CockroachDBConnector) protectSnapshotHistory(ctx context.Context, flowJobName string, systemTime string) {
	conn, err := c.historyProtectionSession(ctx)
	if err != nil {
		c.logger.Warn("[cockroachdb] unable to protect snapshot timestamp", slog.Any("error", err))
		return
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			c.logger.Debug("[cockroachdb] failed to close history protection connection", slog.Any("error", err))
		}
	}()

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

// maybeExtendSnapshotHistory fires a best-effort extension of the mirror's
// snapshot history protection from a QRep partition pull, at most once per
// historyRetentionExtendInterval per connector.
func (c *CockroachDBConnector) maybeExtendSnapshotHistory(ctx context.Context, config *protos.QRepConfig) {
	if shouldExtendSnapshotHistory(c.Config, config) &&
		acquireHistoryExtendSlot(&c.historyExtendedAt, time.Now().UnixNano()) {
		c.extendSnapshotHistoryProtection(ctx, config.ParentMirrorName)
	}
}

// shouldExtendSnapshotHistory reports whether a QRep pull can have a snapshot
// protection job to extend: protection is only ever created for changefeed
// mirrors (shouldProtectSnapshotHistory) under the parent mirror's name, and
// a snapshot name in the CockroachDB HLC form marks the pull as part of that
// mirror's initial snapshot reading at the protected timestamp.
func shouldExtendSnapshotHistory(config *protos.CockroachDBConfig, qrepConfig *protos.QRepConfig) bool {
	return config.UseChangefeeds &&
		qrepConfig.ParentMirrorName != "" &&
		crdbSystemTimeRegex.MatchString(qrepConfig.SnapshotName)
}

// acquireHistoryExtendSlot throttles extensions to at most one per
// historyRetentionExtendInterval, atomically claiming the slot so concurrent
// partition pulls on the same connector do not stack extensions. lastNanos
// starting at zero makes the first pull always claim a slot.
func acquireHistoryExtendSlot(lastNanos *atomic.Int64, nowNanos int64) bool {
	last := lastNanos.Load()
	return nowNanos-last >= int64(historyRetentionExtendInterval) && lastNanos.CompareAndSwap(last, nowNanos)
}

// extendSnapshotHistoryProtection heartbeats the flow's running HISTORY
// RETENTION job, pushing its expiry to now + historyRetentionWindow so an
// initial load that outlives the window keeps its MVCC history. Fully
// best-effort like the create and release paths: a snapshot that cannot
// extend still has whatever remains of the current window.
func (c *CockroachDBConnector) extendSnapshotHistoryProtection(ctx context.Context, flowJobName string) {
	conn, err := c.historyProtectionSession(ctx)
	if err != nil {
		c.logger.Warn("[cockroachdb] unable to extend snapshot history protection", slog.Any("error", err))
		return
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			c.logger.Debug("[cockroachdb] failed to close history protection connection", slog.Any("error", err))
		}
	}()

	var jobID int64
	if err := conn.QueryRow(ctx, historyRetentionLookupQuery,
		historyRetentionDescription(flowJobName)).Scan(&jobID); err != nil {
		// no running job is normal: protection creation may have failed, the
		// window may have expired, or a changefeed session already released it
		if !errors.Is(err, pgx.ErrNoRows) {
			c.logger.Warn("[cockroachdb] failed to look up snapshot history retention job", slog.Any("error", err))
		}
		return
	}
	if _, err := conn.Exec(ctx, historyRetentionExtendQuery, jobID); err != nil {
		c.logger.Warn("[cockroachdb] could not extend snapshot history protection,"+
			" the initial load keeps the remainder of the current window",
			slog.Int64("jobID", jobID), slog.Any("error", err))
		return
	}
	c.logger.Info("[cockroachdb] extended snapshot history protection",
		slog.Int64("jobID", jobID),
		slog.Duration("expiresAfter", historyRetentionWindow))
}

// releaseSnapshotHistoryProtection cancels the flow's HISTORY RETENTION job
// if one is still running. The job is found by its description, so nothing
// has to be persisted and repeated calls are safe no-ops.
func (c *CockroachDBConnector) releaseSnapshotHistoryProtection(ctx context.Context, flowJobName string) {
	if !c.Config.UseChangefeeds {
		// protection is only ever created for changefeed mirrors
		// (shouldProtectSnapshotHistory), so there is nothing to release and no
		// reason to scan the cluster's jobs
		return
	}
	var jobID int64
	if err := c.conn.QueryRow(ctx, historyRetentionLookupQuery,
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
