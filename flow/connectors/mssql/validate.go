package connmssql

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peerdb/flow/connectors/postgres/sanitize"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

func (c *MsSqlConnector) ValidateCheck(ctx context.Context) error {
	return c.conn.PingContext(ctx)
}

func (c *MsSqlConnector) ValidateMirrorSource(ctx context.Context, cfg *protos.FlowConnectionConfigsCore) error {
	sourceTables := make([]*common.QualifiedTable, 0, len(cfg.TableMappings))
	for _, tm := range cfg.TableMappings {
		parsed, err := common.ParseTableIdentifier(tm.SourceTableIdentifier)
		if err != nil {
			return fmt.Errorf("invalid source table identifier: %w", err)
		}
		sourceTables = append(sourceTables, parsed)
	}

	if err := c.checkSourceTables(ctx, sourceTables); err != nil {
		return fmt.Errorf("provided source tables invalidated: %w", err)
	}

	if cfg.DoInitialSnapshot && cfg.InitialSnapshotOnly {
		return nil
	}

	if err := c.checkCdcEnabled(ctx); err != nil {
		return err
	}

	for _, t := range sourceTables {
		if err := c.checkTableCdcEnabled(ctx, t); err != nil {
			return err
		}
	}

	return nil
}

func (c *MsSqlConnector) checkSourceTables(ctx context.Context, tables []*common.QualifiedTable) error {
	for _, t := range tables {
		row := c.conn.QueryRowContext(ctx, "SELECT TOP 0 * FROM "+t.MsSql()) //nolint:gosec // quoted identifier
		if err := row.Err(); err != nil {
			return fmt.Errorf("error checking table %s: %w", t.MsSql(), err)
		}
	}
	return nil
}

func (c *MsSqlConnector) checkCdcEnabled(ctx context.Context) error {
	var enabled bool
	err := c.conn.QueryRowContext(ctx,
		"SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()").Scan(&enabled)
	if err != nil {
		return fmt.Errorf("[mssql] failed to check CDC status: %w", err)
	}
	if !enabled {
		return fmt.Errorf("[mssql] CDC not enabled on database, run: EXEC sys.sp_cdc_enable_db")
	}
	return nil
}

func (c *MsSqlConnector) checkTableCdcEnabled(ctx context.Context, t *common.QualifiedTable) error {
	var tracked bool
	err := c.conn.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT is_tracked_by_cdc FROM sys.tables WHERE object_id = OBJECT_ID(%s)",
		sanitize.QuoteString(t.Namespace+"."+t.Table))).Scan(&tracked)
	if err != nil {
		return fmt.Errorf("[mssql] failed to check CDC for %s.%s: %w", t.Namespace, t.Table, err)
	}
	if !tracked {
		return fmt.Errorf(
			"[mssql] CDC not enabled on table %s.%s, run: "+
				"EXEC sys.sp_cdc_enable_table @source_schema=N'%s', @source_name=N'%s', @role_name=NULL",
			t.Namespace, t.Table, t.Namespace, t.Table)
	}
	return nil
}
