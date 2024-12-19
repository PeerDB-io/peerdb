package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.temporal.io/sdk/log"
)

var acceptableTableEngines = []string{"ReplacingMergeTree", "MergeTree"}

func CheckIfClickHouseCloudHasSharedMergeTreeEnabled(ctx context.Context, logger log.Logger,
	conn clickhouse.Conn,
) error {
	// this is to indicate ClickHouse Cloud service is now creating tables with Shared* by default
	var cloudModeEngine bool
	if err := QueryRow(ctx, logger, conn,
		"SELECT value='2' AND changed='1' AND readonly='1' FROM system.settings WHERE name = 'cloud_mode_engine'").
		Scan(&cloudModeEngine); err != nil {
		return fmt.Errorf("failed to validate cloud_mode_engine setting: %w", err)
	}
	if !cloudModeEngine {
		return errors.New("ClickHouse service is not migrated to use SharedMergeTree tables, please contact support")
	}
	return nil
}

func CheckIfTablesEmptyAndEngine(ctx context.Context, logger log.Logger, conn clickhouse.Conn,
	tables []string, initialSnapshotEnabled bool, checkForCloudSMT bool,
) error {
	queryInput := make([]interface{}, 0, len(tables))
	for _, table := range tables {
		queryInput = append(queryInput, table)
	}
	rows, err := Query(ctx, logger, conn,
		fmt.Sprintf("SELECT name,engine,total_rows FROM system.tables WHERE database=currentDatabase() AND name IN (%s)",
			strings.Join(slices.Repeat([]string{"?"}, len(tables)), ",")), queryInput...)
	if err != nil {
		return fmt.Errorf("failed to get information for destination tables: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName, engine string
		var totalRows uint64
		if err := rows.Scan(&tableName, &engine, &totalRows); err != nil {
			return fmt.Errorf("failed to scan information for tables: %w", err)
		}
		if totalRows != 0 && initialSnapshotEnabled {
			return fmt.Errorf("table %s exists and is not empty", tableName)
		}
		if !slices.Contains(acceptableTableEngines, strings.TrimPrefix(engine, "Shared")) {
			logger.Warn("[clickhouse] table engine not explicitly supported",
				slog.String("table", tableName), slog.String("engine", engine))
		}
		if checkForCloudSMT && !strings.HasPrefix(engine, "Shared") {
			return fmt.Errorf("table %s exists and does not use SharedMergeTree engine", tableName)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to read rows: %w", err)
	}

	return nil
}

type ClickHouseColumn struct {
	Name string
	Type string
}

func GetTableColumnsMapping(ctx context.Context, logger log.Logger, conn clickhouse.Conn,
	tables []string,
) (map[string][]ClickHouseColumn, error) {
	tableColumnsMapping := make(map[string][]ClickHouseColumn, len(tables))
	queryInput := make([]interface{}, 0, len(tables))
	for _, table := range tables {
		queryInput = append(queryInput, table)
	}
	rows, err := Query(ctx, logger, conn,
		fmt.Sprintf("SELECT name,type,table FROM system.columns WHERE database=currentDatabase() AND table IN (%s)",
			strings.Join(slices.Repeat([]string{"?"}, len(tables)), ",")), queryInput...)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns for destination tables: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var tableName string
		var clickhouseColumn ClickHouseColumn
		if err := rows.Scan(&clickhouseColumn.Name, &clickhouseColumn.Type, &tableName); err != nil {
			return nil, fmt.Errorf("failed to scan columns for tables: %w", err)
		}
		tableColumnsMapping[tableName] = append(tableColumnsMapping[tableName], clickhouseColumn)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read rows: %w", err)
	}

	return tableColumnsMapping, nil
}
