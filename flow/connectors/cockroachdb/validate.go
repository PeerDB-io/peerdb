package conncockroachdb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

func (c *CockroachDBConnector) ValidateCheck(ctx context.Context) error {
	majorVersion, err := c.GetMajorVersion(ctx)
	if err != nil {
		return err
	}

	if majorVersion < 22 {
		return fmt.Errorf("CockroachDB must be version 22.1 or above. Current version: %d.x", majorVersion)
	}
	return nil
}

func (c *CockroachDBConnector) ValidateMirrorSource(ctx context.Context, cfg *protos.FlowConnectionConfigsCore) error {
	var missingTables []common.QualifiedTable
	parsedTables := make([]*common.QualifiedTable, 0, len(cfg.TableMappings))
	for _, tm := range cfg.TableMappings {
		parsedTable, err := common.ParseTableIdentifier(tm.SourceTableIdentifier)
		if err != nil {
			return fmt.Errorf("invalid source table identifier %s: %w", tm.SourceTableIdentifier, err)
		}
		parsedTables = append(parsedTables, parsedTable)
		var exists bool
		if err := c.conn.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = $1 AND table_name = $2 AND table_type = 'BASE TABLE'
		)`, parsedTable.Namespace, parsedTable.Table).Scan(&exists); err != nil {
			return fmt.Errorf("failed to check source table %s: %w", tm.SourceTableIdentifier, err)
		}
		if !exists {
			missingTables = append(missingTables, *parsedTable)
		}
	}
	if len(missingTables) > 0 {
		return common.NewSourceTablesMissingError(missingTables)
	}

	if cfg.DoInitialSnapshot {
		if err := c.validateSnapshotGCTTL(ctx, parsedTables); err != nil {
			return err
		}
	}

	// snapshot-only mirrors never open a changefeed
	if cfg.DoInitialSnapshot && cfg.InitialSnapshotOnly {
		return nil
	}

	if !c.Config.UseChangefeeds {
		return errors.New("CDC mirrors from CockroachDB use changefeeds:" +
			" enable changefeeds on the CockroachDB peer or create an initial-snapshot-only mirror")
	}
	return c.checkRangefeedEnabled(ctx)
}

// snapshotGCTTLFloorSeconds is the hard lower bound on the effective
// gc.ttlseconds of source tables for mirrors with an initial snapshot: the
// snapshot reads AS OF SYSTEM TIME at one fixed timestamp, and those reads
// fail once the timestamp falls behind the replica GC threshold. Snapshot
// duration cannot be predicted reliably, but a window this small cannot
// cover any realistic initial load.
const snapshotGCTTLFloorSeconds = 600

var gcTTLSecondsRegex = regexp.MustCompile(`gc\.ttlseconds\s*=\s*(\d+)`)

// parseGCTTLSeconds extracts gc.ttlseconds from the raw_config_sql column of
// SHOW ZONE CONFIGURATION output.
func parseGCTTLSeconds(rawConfigSQL string) (int64, bool) {
	match := gcTTLSecondsRegex.FindStringSubmatch(rawConfigSQL)
	if match == nil {
		return 0, false
	}
	ttl, err := strconv.ParseInt(match[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return ttl, true
}

func gcTTLFloorError(table string, target string, ttlSeconds int64) error {
	if ttlSeconds >= snapshotGCTTLFloorSeconds {
		return nil
	}
	return fmt.Errorf("effective gc.ttlseconds for table %s is %d (configured on %s), below the %d second minimum"+
		" for mirrors with an initial snapshot: snapshot reads run AS OF SYSTEM TIME at one fixed timestamp and fail"+
		" once it falls behind the replica GC threshold; raise it to cover the expected initial load duration with"+
		" ALTER TABLE %s CONFIGURE ZONE USING gc.ttlseconds = <seconds>",
		table, ttlSeconds, target, snapshotGCTTLFloorSeconds, table)
}

// validateSnapshotGCTTL checks that the MVCC GC window of every source table
// leaves the initial snapshot's AS OF SYSTEM TIME reads room to complete,
// failing hard below snapshotGCTTLFloorSeconds and warning with sizing
// guidance otherwise.
func (c *CockroachDBConnector) validateSnapshotGCTTL(ctx context.Context, tables []*common.QualifiedTable) error {
	minTTL := int64(-1)
	var minTTLTable, minTTLTarget string
	var totalRowEstimate int64
	for _, table := range tables {
		ttl, target, err := c.effectiveGCTTLSeconds(ctx, table)
		if err != nil {
			// insufficient privileges to read zone configurations shouldn't block
			// mirror creation; the snapshot surfaces GC threshold errors otherwise
			c.logger.Warn("[cockroachdb] unable to determine effective gc.ttlseconds",
				slog.String("table", table.String()), slog.Any("error", err))
			continue
		}
		if err := gcTTLFloorError(table.String(), target, ttl); err != nil {
			return err
		}
		if minTTL < 0 || ttl < minTTL {
			minTTL, minTTLTable, minTTLTarget = ttl, table.String(), target
		}
		if estimate, err := c.tableRowEstimate(ctx, table); err != nil {
			c.logger.Warn("[cockroachdb] failed to estimate row count",
				slog.String("table", table.String()), slog.Any("error", err))
		} else {
			totalRowEstimate += estimate
		}
	}
	if minTTL >= 0 {
		c.logger.Warn("[cockroachdb] initial snapshot reads AS OF SYSTEM TIME at one fixed timestamp"+
			" and fails if the initial load outlasts the MVCC GC window;"+
			" size gc.ttlseconds on the source tables to cover the expected initial load duration",
			slog.Int64("minGcTtlSeconds", minTTL),
			slog.String("minGcTtlTable", minTTLTable),
			slog.String("minGcTtlConfiguredOn", minTTLTarget),
			slog.Int64("totalRowEstimate", totalRowEstimate))
	}
	return nil
}

// effectiveGCTTLSeconds resolves the gc.ttlseconds that applies to a table.
// SHOW ZONE CONFIGURATION FOR TABLE already returns the effective cascaded
// zone config with its origin (table, database or RANGE default) in the
// target column; the database and default-range lookups are fallbacks for
// when the table-level statement itself fails.
func (c *CockroachDBConnector) effectiveGCTTLSeconds(
	ctx context.Context, table *common.QualifiedTable,
) (int64, string, error) {
	queries := []string{
		"SHOW ZONE CONFIGURATION FOR TABLE " + table.String(),
		"SHOW ZONE CONFIGURATION FOR DATABASE " + common.QuoteIdentifier(c.Config.Database),
		"SHOW ZONE CONFIGURATION FOR RANGE default",
	}
	var lastErr error
	for _, query := range queries {
		var target, rawConfigSQL string
		if err := c.conn.QueryRow(ctx, query).Scan(&target, &rawConfigSQL); err != nil {
			lastErr = err
			continue
		}
		if ttl, ok := parseGCTTLSeconds(rawConfigSQL); ok {
			return ttl, target, nil
		}
		lastErr = fmt.Errorf("gc.ttlseconds not present in zone configuration for %s", target)
	}
	return 0, "", lastErr
}

func (c *CockroachDBConnector) checkRangefeedEnabled(ctx context.Context) error {
	// crdb_internal.cluster_settings is gated behind allow_unsafe_internals on
	// v26.1+; SHOW CLUSTER SETTING only needs VIEWCLUSTERSETTING
	var value any
	if err := c.conn.QueryRow(ctx, "SHOW CLUSTER SETTING kv.rangefeed.enabled").Scan(&value); err != nil {
		// insufficient privileges to read cluster settings shouldn't block
		// mirror creation; changefeed creation surfaces the error otherwise
		c.logger.Warn("[cockroachdb] unable to check kv.rangefeed.enabled", slog.Any("error", err))
		return nil
	}
	if !settingValueIsTrue(value) {
		return errors.New("changefeeds require rangefeeds: run SET CLUSTER SETTING kv.rangefeed.enabled = true;" +
			" rangefeeds are enabled by default on CockroachDB Cloud, self-hosted clusters must enable them explicitly")
	}
	return nil
}

func settingValueIsTrue(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return v == "true" || v == "t" || v == "on"
	default:
		return false
	}
}

func (c *CockroachDBConnector) GetDatabaseVariant(ctx context.Context) (protos.DatabaseVariant, error) {
	// SHOW CLUSTER SETTING works without allow_unsafe_internals, unlike
	// crdb_internal.cluster_settings
	var clusterOrg string
	err := c.conn.QueryRow(ctx, "SHOW CLUSTER SETTING cluster.organization").Scan(&clusterOrg)
	if err != nil {
		return protos.DatabaseVariant_VARIANT_UNKNOWN, nil
	}

	// server.serverless.enabled only exists on serverless hosts
	var serverless any
	err = c.conn.QueryRow(ctx, "SHOW CLUSTER SETTING server.serverless.enabled").Scan(&serverless)
	if err == nil && settingValueIsTrue(serverless) {
		return protos.DatabaseVariant_COCKROACHDB_SERVERLESS, nil
	}

	// managed CockroachDB Cloud clusters always have cluster.organization set
	if clusterOrg != "" {
		return protos.DatabaseVariant_COCKROACHDB_CLOUD, nil
	}

	return protos.DatabaseVariant_VARIANT_UNKNOWN, nil
}
