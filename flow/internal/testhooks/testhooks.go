// Package testhooks injects faults at fixed sites in the sync and normalize paths.
//
// Hooks do nothing unless the worker process sets PEERDB_TEST_HOOKS=true.
// A test arms a site for one flow by inserting a row into the catalog table
// peerdb_test_hooks. Each armed row fires at most once, across activity
// retries and worker processes.
package testhooks

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

const (
	EnvEnabled = "PEERDB_TEST_HOOKS"
	tableName  = "peerdb_test_hooks"

	pausePollInterval = 100 * time.Millisecond
)

var ErrInjected = errors.New("testhooks: injected failure")

var enabled = sync.OnceValue(func() bool {
	v, _ := strconv.ParseBool(os.Getenv(EnvEnabled))
	return v
})

type tableKey struct{}

// WithTable scopes hooks reached under ctx to one table, for per-table loops.
// A site armed for a table fires only there, and records the table it fired for.
func WithTable(ctx context.Context, table string) context.Context {
	return context.WithValue(ctx, tableKey{}, table)
}

func tableFromCtx(ctx context.Context) string {
	table, _ := ctx.Value(tableKey{}).(string)
	return table
}

// claimSQL fires an armed row at most once. Under READ COMMITTED a concurrent
// claimer re-checks fired_at after the row lock, so only one caller wins.
const claimSQL = `
	UPDATE ` + tableName + ` h SET fired_at = now(), fired_table = NULLIF($3, '')
	WHERE h.flow_name = $1 AND h.hook = $2 AND h.fired_at IS NULL
	  AND (h.table_filter IS NULL OR h.table_filter = NULLIF($3, ''))
	  AND (h.after_hook IS NULL OR EXISTS (
	    SELECT 1 FROM ` + tableName + ` a
	    WHERE a.flow_name = h.flow_name AND a.hook = h.after_hook AND a.fired_at IS NOT NULL))
	RETURNING h.action`

// Failpoint returns ErrInjected the first time an armed fail site is reached.
func Failpoint(ctx context.Context, site Site) error {
	if !enabled() {
		return nil
	}
	fired, err := claim(ctx, site)
	if err != nil {
		return err
	}
	if !fired {
		return nil
	}
	internal.LoggerFromCtx(ctx).Warn("testhooks: injecting failure", slog.String("site", site.Name))
	return fmt.Errorf("%w at %s", ErrInjected, site.Name)
}

// Pausepoint blocks the first time an armed pause site is reached, until the test
// releases it or ctx ends. Errors are logged rather than returned, so a pause site
// cannot change control flow.
func Pausepoint(ctx context.Context, site Site) {
	if !enabled() {
		return
	}
	logger := internal.LoggerFromCtx(ctx)
	fired, err := claim(ctx, site)
	if err != nil {
		logger.Error("testhooks: failed to claim pause", slog.String("site", site.Name), slog.Any("error", err))
		return
	}
	if !fired {
		return
	}
	logger.Warn("testhooks: paused", slog.String("site", site.Name))
	flowName := flowNameFromCtx(ctx)
	for {
		released, err := isReleased(ctx, flowName, site)
		if err != nil {
			logger.Error("testhooks: failed to check release", slog.String("site", site.Name), slog.Any("error", err))
		} else if released {
			logger.Warn("testhooks: released", slog.String("site", site.Name))
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(pausePollInterval):
		}
	}
}

func flowNameFromCtx(ctx context.Context) string {
	flowName, _ := ctx.Value(shared.FlowNameKey).(string)
	return flowName
}

func claim(ctx context.Context, site Site) (bool, error) {
	flowName := flowNameFromCtx(ctx)
	if flowName == "" {
		return false, nil
	}
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return false, fmt.Errorf("testhooks: %w", err)
	}
	var action string
	err = pool.Pool.QueryRow(ctx, claimSQL, flowName, site.Name, tableFromCtx(ctx)).Scan(&action)
	if errors.Is(err, pgx.ErrNoRows) || isUndefinedTable(err) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("testhooks: claim %s: %w", site.Name, err)
	}
	if action != site.Kind.String() {
		return false, fmt.Errorf("testhooks: %s armed as %s but is a %s site", site.Name, action, site.Kind)
	}
	return true, nil
}

func isReleased(ctx context.Context, flowName string, site Site) (bool, error) {
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return false, err
	}
	var released bool
	err = pool.Pool.QueryRow(ctx,
		`SELECT released_at IS NOT NULL FROM `+tableName+` WHERE flow_name = $1 AND hook = $2`,
		flowName, site.Name,
	).Scan(&released)
	return released, err
}

func isUndefinedTable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UndefinedTable
}
