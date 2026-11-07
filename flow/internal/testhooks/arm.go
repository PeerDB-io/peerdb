package testhooks

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DB is the subset of a catalog connection the test helpers need.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// setupSQL runs as one transaction, so the advisory lock serializes parallel
// tests across creation and the column migration for catalogs created by an
// earlier version of the table.
const setupSQL = `
	DO $$ BEGIN
		PERFORM pg_advisory_xact_lock(7265300001);
		CREATE TABLE IF NOT EXISTS ` + tableName + ` (
			flow_name    TEXT NOT NULL,
			hook         TEXT NOT NULL,
			action       TEXT NOT NULL,
			after_hook   TEXT,
			armed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
			fired_at     TIMESTAMPTZ,
			released_at  TIMESTAMPTZ,
			PRIMARY KEY (flow_name, hook)
		);
		ALTER TABLE ` + tableName + `
			ADD COLUMN IF NOT EXISTS table_filter TEXT,
			ADD COLUMN IF NOT EXISTS fired_table TEXT;
	END $$`

// EnsureTable creates the hook table.
func EnsureTable(ctx context.Context, db DB) error {
	_, err := db.Exec(ctx, setupSQL)
	return err
}

// Arming describes one armed site for one flow.
type Arming struct {
	// After makes Site fire only once this other site has fired.
	After *Site
	Flow  string
	// Table, when set, makes Site fire only in the per-table loop for that table.
	Table string
	Site  Site
}

// Arm makes a site fire once.
func Arm(ctx context.Context, db DB, a Arming) error {
	var afterName, table *string
	if a.After != nil {
		afterName = &a.After.Name
	}
	if a.Table != "" {
		table = &a.Table
	}
	_, err := db.Exec(ctx,
		`INSERT INTO `+tableName+` (flow_name, hook, action, after_hook, table_filter) VALUES ($1, $2, $3, $4, $5)`,
		a.Flow, a.Site.Name, a.Site.Kind.String(), afterName, table)
	if err != nil {
		return fmt.Errorf("arm %s: %w", a.Site.Name, err)
	}
	return nil
}

// Fired reports whether site has fired for flowName, and for which table if it
// fired inside a per-table loop. For a pause site, fired means the code is
// paused there or has been released.
func Fired(ctx context.Context, db DB, flowName string, site Site) (bool, string, error) {
	var fired bool
	var table *string
	err := db.QueryRow(ctx,
		`SELECT fired_at IS NOT NULL, fired_table FROM `+tableName+` WHERE flow_name = $1 AND hook = $2`,
		flowName, site.Name,
	).Scan(&fired, &table)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, "", fmt.Errorf("%s is not armed for %s", site.Name, flowName)
	} else if err != nil {
		return false, "", err
	}
	if table == nil {
		return fired, "", nil
	}
	return fired, *table, nil
}

// Release lets a paused site continue.
func Release(ctx context.Context, db DB, flowName string, site Site) error {
	tag, err := db.Exec(ctx,
		`UPDATE `+tableName+` SET released_at = now() WHERE flow_name = $1 AND hook = $2 AND action = 'pause'`,
		flowName, site.Name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%s is not an armed pause site for %s", site.Name, flowName)
	}
	return nil
}

// FiredFails counts the fail sites that have fired for flowName.
func FiredFails(ctx context.Context, db DB, flowName string) (int64, error) {
	var n int64
	err := db.QueryRow(ctx,
		`SELECT count(*) FROM `+tableName+` WHERE flow_name = $1 AND action = 'fail' AND fired_at IS NOT NULL`,
		flowName).Scan(&n)
	return n, err
}
