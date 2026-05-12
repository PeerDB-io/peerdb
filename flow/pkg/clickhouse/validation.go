package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	chvproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/pkg/objectstore"
)

func CheckNotSystemDatabase(database string) error {
	switch database {
	case "system", "information_schema", "INFORMATION_SCHEMA":
		return fmt.Errorf("database %q is a system database and cannot be used as a destination", database)
	}
	return nil
}

var acceptableTableEngines = []string{
	EngineReplacingMergeTree, EngineMergeTree, EngineReplicatedReplacingMergeTree, EngineReplicatedMergeTree,
	EngineCoalescingMergeTree, EngineNull,
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
		return fmt.Errorf("ClickHouse service is not migrated to use SharedMergeTree tables, please contact support")
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
				if engine == "View" || engine == "MaterializedView" {
					return fmt.Errorf("destination table can not be a view")
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

func ValidateClickHouseHost(ctx context.Context, chHost string, allowedDomainString string) error {
	allowedDomains := strings.Split(allowedDomainString, ",")
	if len(allowedDomains) == 0 {
		return nil
	}
	// check if chHost ends with one of the allowed domains
	for _, domain := range allowedDomains {
		if strings.HasSuffix(chHost, domain) {
			return nil
		}
	}
	return fmt.Errorf("invalid ClickHouse host domain: %s. Allowed domains: %s",
		chHost, strings.Join(allowedDomains, ","))
}

func ValidateClickHousePeer(
	ctx context.Context,
	logger log.Logger,
	allowedDomains string,
	serviceHost string,
	conn clickhouse.Conn,
	stagingValidator objectstore.StagingValidator,
) error {
	// Hostname validation
	if err := ValidateClickHouseHost(ctx, serviceHost, allowedDomains); err != nil {
		return err
	}

	// Target service validation

	validateDummyTableName := "peerdb_validation_" + common.RandomString(4)
	validateDummyTableNameRenamed := validateDummyTableName + "_renamed"

	// create a table
	if err := Exec(ctx, logger, conn,
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id UInt64) ENGINE = ReplacingMergeTree ORDER BY id;`, validateDummyTableName),
	); err != nil {
		return fmt.Errorf("failed to create validation table %s: %w", validateDummyTableName, err)
	}
	defer func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		for _, table := range []string{validateDummyTableName, validateDummyTableNameRenamed} {
			for attempt := range 3 {
				if attempt > 0 {
					time.Sleep(time.Duration(attempt) * 2 * time.Second)
				}
				err := Exec(dropCtx, logger, conn, "DROP TABLE IF EXISTS "+table)
				if err == nil {
					break
				}
				var chException *clickhouse.Exception
				if errors.As(err, &chException) && chproto.Error(chException.Code) == chproto.ErrUnfinished {
					logger.Warn("validation drop table blocked by in-flight DDL, retrying",
						slog.String("table", table), slog.Int("attempt", attempt+1))
					continue
				}
				logger.Error("validation failed to drop table", slog.String("table", table), slog.Any("error", err))
				break
			}
		}
	}()

	// add a column
	if err := Exec(ctx, logger, conn,
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN updated_at DateTime64(9) DEFAULT now64()", validateDummyTableName),
	); err != nil {
		return fmt.Errorf("failed to add column to validation table %s: %w", validateDummyTableName, err)
	}

	// rename the table
	if err := Exec(ctx, logger, conn,
		fmt.Sprintf("RENAME TABLE %s TO %s", validateDummyTableName, validateDummyTableNameRenamed),
	); err != nil {
		return fmt.Errorf("failed to rename validation table %s: %w", validateDummyTableName, err)
	}

	// insert a row
	if err := Exec(ctx, logger, conn, fmt.Sprintf("INSERT INTO %s VALUES (1, now64())", validateDummyTableNameRenamed)); err != nil {
		return fmt.Errorf("failed to insert into validation table %s: %w", validateDummyTableNameRenamed, err)
	}

	// drop the table
	if err := Exec(ctx, logger, conn, "DROP TABLE IF EXISTS "+validateDummyTableNameRenamed); err != nil {
		return fmt.Errorf("failed to drop validation table %s: %w", validateDummyTableNameRenamed, err)
	}

	// Staging validation

	// validate staging storage
	if err := stagingValidator(ctx); err != nil {
		return fmt.Errorf("failed to validate staging bucket: %w", err)
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

func ValidateOrderingKeys(ctx context.Context, logger log.Logger, conn clickhouse.Conn,
	chVersion *chvproto.Version, sourceTable string,
	hasPrimaryKeys bool, sortingKeys []string, engine string,
) error {
	if hasPrimaryKeys || len(sortingKeys) > 0 {
		return nil
	}
	if engine == EngineNull || engine == EngineMergeTree {
		return nil
	}
	if chVersion == nil || !chvproto.CheckMinVersion(chvproto.Version{Major: 25, Minor: 12, Patch: 0}, *chVersion) {
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
