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
	// its deterministic description. It reads crdb_internal.jobs, which holds
	// the full description (verified on v24.1, v25.4 and v26.2), because SHOW
	// JOBS middle-truncates descriptions around 69 characters and the suffix
	// match would then never find the job of a longer flow name. The exact
	// suffix comparison (instead of a LIKE pattern) keeps `_` and `%` in flow
	// names from acting as wildcards and matching another mirror's job. The
	// ::text casts are load-bearing: without them CockroachDB cannot infer
	// the placeholder type inside length() and fails with SQLSTATE 42P18.
	historyRetentionLookupQuery = "SELECT job_id FROM crdb_internal.jobs WHERE job_type = 'HISTORY RETENTION'" +
		" AND status = 'running' AND right(description, length($1::text)) = $1::text LIMIT 1"

	// historyRetentionLookupFallbackQuery is the SHOW JOBS form of the lookup
	// for sessions that cannot read crdb_internal.jobs (v26.1+ restricts
	// crdb_internal behind allow_unsafe_internals, which historyProtectionSession
	// sets best-effort). SHOW JOBS truncates long descriptions, so the fallback
	// can miss jobs of long flow names; those still expire with the orphan
	// window like every other unreleased job.
	historyRetentionLookupFallbackQuery = "SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'HISTORY RETENTION'" +
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

// lookupHistoryRetentionJob resolves the flow's running HISTORY RETENTION job
// id by its description, preferring crdb_internal.jobs for the untruncated
// description and degrading to SHOW JOBS when the internal table is not
// readable on this session. Returns pgx.ErrNoRows when no job matches.
func lookupHistoryRetentionJob(ctx context.Context, conn *crdbConn, flowJobName string) (int64, error) {
	description := historyRetentionDescription(flowJobName)
	var jobID int64
	err := conn.QueryRow(ctx, historyRetentionLookupQuery, description).Scan(&jobID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		err = conn.QueryRow(ctx, historyRetentionLookupFallbackQuery, description).Scan(&jobID)
	}
	return jobID, err
}

func protectMVCCHistoryQuery(window time.Duration) string {
	return fmt.Sprintf("SELECT crdb_internal.protect_mvcc_history($1::decimal, '%d seconds'::interval, $2)",
		int64(window.Seconds()))
}

// historyProtectionSession opens a dedicated connection for the crdb_internal
// history retention builtins and the crdb_internal.jobs lookup so
// allow_unsafe_internals does not linger on the shared connection. v26.1+
// gates crdb_internal behind that session variable; 25.x has it defaulting to
// allow and older versions do not know it at all, so failing to set it only
// logs (the job lookup then degrades to its SHOW JOBS fallback).
func (c *CockroachDBConnector) historyProtectionSession(ctx context.Context) (*crdbConn, error) {
	connConfig, err := ParseConfig(c.connStr, c.config)
	if err != nil {
		return nil, err
	}
	conn, err := NewCockroachDBConnFromConfig(ctx, connConfig, c.ssh)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Exec(ctx, "SET allow_unsafe_internals = on"); err != nil {
		c.logger.Warn("[cockroachdb] could not set allow_unsafe_internals", slog.Any("error", err))
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
	if shouldExtendSnapshotHistory(config) &&
		acquireHistoryExtendSlot(&c.historyExtendedAt, time.Now().UnixNano()) {
		c.extendSnapshotHistoryProtection(ctx, config.ParentMirrorName)
	}
}

// shouldExtendSnapshotHistory reports whether a QRep pull can have a snapshot
// protection job to extend: protection is only ever created by
// SetupReplication under the parent mirror's name, and a snapshot name in the
// CockroachDB HLC form marks the pull as part of that mirror's initial
// snapshot reading at the protected timestamp.
func shouldExtendSnapshotHistory(qrepConfig *protos.QRepConfig) bool {
	return qrepConfig.ParentMirrorName != "" &&
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

	jobID, err := lookupHistoryRetentionJob(ctx, conn, flowJobName)
	if err != nil {
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
	conn, err := c.historyProtectionSession(ctx)
	if err != nil {
		c.logger.Warn("[cockroachdb] unable to release snapshot history protection", slog.Any("error", err))
		return
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			c.logger.Debug("[cockroachdb] failed to close history protection connection", slog.Any("error", err))
		}
	}()
	jobID, err := lookupHistoryRetentionJob(ctx, conn, flowJobName)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			c.logger.Warn("[cockroachdb] failed to look up snapshot history retention job", slog.Any("error", err))
		}
		return
	}
	if _, err := conn.Exec(ctx, "CANCEL JOB $1", jobID); err != nil {
		c.logger.Warn("[cockroachdb] failed to cancel snapshot history retention job",
			slog.Int64("jobID", jobID), slog.Any("error", err))
		return
	}
	c.logger.Info("[cockroachdb] released snapshot history retention job", slog.Int64("jobID", jobID))
}
