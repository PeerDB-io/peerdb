package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"go.temporal.io/sdk/log"
)

func CheckNotSystemDatabase(database string) error {
	switch database {
	case "system", "information_schema", "INFORMATION_SCHEMA":
		return fmt.Errorf("database %q is a system database and cannot be used as a destination", database)
	}
	return nil
}

var acceptableTableEngines = []string{
	"ReplacingMergeTree", "MergeTree", "ReplicatedReplacingMergeTree", "ReplicatedMergeTree", "CoalescingMergeTree", "Null",
}

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
	tables []string, initialSnapshotEnabled bool, checkForCloudSMT bool, allowNonEmpty bool,
) error {
	queryTables := make([]string, 0, min(len(tables), 200))

	for chunk := range slices.Chunk(tables, 200) {
		if err := func() error {
			queryTables = queryTables[:0]
			for _, table := range chunk {
				queryTables = append(queryTables, QuoteLiteral(table))
			}

			rows, err := Query(ctx, logger, conn,
				fmt.Sprintf("SELECT name,engine,total_rows FROM system.tables WHERE database=currentDatabase() AND name IN (%s)",
					strings.Join(queryTables, ",")))
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
				if !allowNonEmpty && totalRows != 0 && initialSnapshotEnabled {
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
		}(); err != nil {
			return err
		}
	}

	return nil
}

type ClickHouseColumn struct {
	Name        string
	Type        string
	DefaultKind string
}

func GetTableColumnsMapping(ctx context.Context, logger log.Logger, conn clickhouse.Conn,
	tables []string,
) (map[string][]ClickHouseColumn, error) {
	tableColumnsMapping := make(map[string][]ClickHouseColumn, len(tables))
	queryTables := make([]string, 0, min(len(tables), 200))

	for chunk := range slices.Chunk(tables, 200) {
		queryTables = queryTables[:0]
		for _, table := range chunk {
			queryTables = append(queryTables, QuoteLiteral(table))
		}

		if err := storeColumnInfoForTable(ctx, logger, conn, queryTables, tableColumnsMapping); err != nil {
			return nil, fmt.Errorf("failed to get columns for destination tables in chunk: %w", err)
		}
	}

	return tableColumnsMapping, nil
}

func storeColumnInfoForTable(ctx context.Context, logger log.Logger, conn clickhouse.Conn,
	queryTables []string,
	tableColumnsMapping map[string][]ClickHouseColumn,
) error {
	rows, err := Query(ctx, logger, conn,
		fmt.Sprintf("SELECT name,type,default_kind,table FROM system.columns WHERE database=currentDatabase() AND table IN (%s)",
			strings.Join(queryTables, ",")))
	if err != nil {
		return fmt.Errorf("failed to get columns for destination tables: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var tableName string
		var clickhouseColumn ClickHouseColumn
		if err := rows.Scan(&clickhouseColumn.Name, &clickhouseColumn.Type, &clickhouseColumn.DefaultKind, &tableName); err != nil {
			return fmt.Errorf("failed to scan columns for tables: %w", err)
		}
		tableColumnsMapping[tableName] = append(tableColumnsMapping[tableName], clickhouseColumn)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to read rows: %w", err)
	}

	return nil
}

func CheckEmptyOrderingKeySupported(ctx context.Context, logger log.Logger, conn clickhouse.Conn,
	chVersion *chproto.Version, sourceTable string,
) error {
	if chVersion == nil || !chproto.CheckMinVersion(chproto.Version{Major: 25, Minor: 12, Patch: 0}, *chVersion) {
		return nil
	}
	var settingVal string
	err := QueryRow(ctx, logger, conn, "SELECT value FROM system.settings WHERE name = 'allow_suspicious_primary_key'").Scan(&settingVal)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to query ClickHouse settings: %w", err)
	}
	if settingVal == "1" {
		return nil
	}

	return fmt.Errorf(
		"cannot determine ORDER BY key from source table %s; empty sort key is not supported",
		sourceTable,
	)
}
