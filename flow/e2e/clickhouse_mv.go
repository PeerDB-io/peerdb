package e2e

import (
	"context"
	"fmt"

	connclickhouse "github.com/PeerDB-io/peerdb/flow/connectors/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// ClickHouseMVManager manages materialized views for ClickHouse testing
type ClickHouseMVManager struct {
	config    *protos.ClickhouseConfig
	tableName string
	mvName    string
	suffix    string
}

// NewClickHouseMVManager creates a new ClickHouseMVManager instance
func newClickHouseMVManager(
	config *protos.ClickhouseConfig,
	tableName string,
	suffix string,
) *ClickHouseMVManager {
	return &ClickHouseMVManager{
		config:    config,
		tableName: tableName,
		mvName:    fmt.Sprintf("%s_mv_%s", tableName, suffix),
		suffix:    suffix,
	}
}

// CreateBadMV creates a materialized view on the configured table
// that will fail when data is inserted.
// Source schema is assumed to correspond to (id UInt64, val String).
func (m *ClickHouseMVManager) CreateBadMV(ctx context.Context) error {
	ch, err := connclickhouse.Connect(ctx, nil, m.config)
	if err != nil {
		return err
	}
	defer ch.Close()

	targetName := fmt.Sprintf("%s_mv_target_%s", m.tableName, m.suffix)
	// Create target table for the MV: val is UInt32 (incompatible with arbitrary strings)
	targetTableSQL := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s (
            id   Int64,
            val UInt32
        )
        ENGINE = MergeTree()
        ORDER BY id`, targetName)
	err = ch.Exec(ctx, targetTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create MV target table: %w", err)
	}

	// Create materialized view that will fail when val is not a parseable integer
	// This uses toUInt32() which throws an exception for non-numeric strings
	mvSQL := fmt.Sprintf(`
        CREATE MATERIALIZED VIEW IF NOT EXISTS %s TO %s
        AS
        SELECT
            1 as id,
			throwIf(1, 'Intentional MV failure') as val
        FROM %s`, m.mvName, targetName, m.tableName)
	err = ch.Exec(ctx, mvSQL)
	if err != nil {
		return fmt.Errorf("failed to create materialized view: %w", err)
	}

	return nil
}

// DropBadMV removes the materialized view and its target table
func (m *ClickHouseMVManager) DropBadMV(ctx context.Context) error {
	ch, err := connclickhouse.Connect(ctx, nil, m.config)
	if err != nil {
		return err
	}
	defer ch.Close()

	dropMVSQL := "DROP VIEW IF EXISTS " + m.mvName
	err = ch.Exec(ctx, dropMVSQL)
	if err != nil {
		return fmt.Errorf("failed to drop materialized view: %w", err)
	}

	return nil
}

func (s ClickHouseSuite) NewMVManager(tableName string, suffix string) *ClickHouseMVManager {
	return newClickHouseMVManager(
		s.Peer().GetClickhouseConfig(),
		tableName,
		suffix,
	)
}
