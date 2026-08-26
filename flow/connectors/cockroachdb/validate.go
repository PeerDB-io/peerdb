package conncockroachdb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

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

	if err := c.validateGCTTL(ctx, parsedTables, cfg.DoInitialSnapshot); err != nil {
		return err
	}

	// snapshot-only mirrors never open a changefeed
	if cfg.DoInitialSnapshot && cfg.InitialSnapshotOnly {
		return nil
	}

	// every other mirror streams CDC through a changefeed, which requires
	// rangefeeds on the cluster
	return c.checkRangefeedEnabled(ctx)
}

// gcTTLFloorSeconds is the hard lower bound on the effective gc.ttlseconds of
// source tables: the initial snapshot reads AS OF SYSTEM TIME at one fixed
// timestamp, the changefeed resumes from its last resolved timestamp, and
// both fail once that timestamp falls behind the replica GC threshold. A full
// day leaves room for long initial loads and paused mirrors; CockroachDB's
// default is 25 hours, so tables only trip this floor when it was lowered
// explicitly.
const gcTTLFloorSeconds = 24 * 60 * 60

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

func validateGCTTLFloor(table string, target string, ttlSeconds int64) error {
	if ttlSeconds >= gcTTLFloorSeconds {
		return nil
	}
	// a partition override cannot be raised through the table's zone config,
	// so the hint has to name the partition in that case
	alterTarget := "TABLE " + table
	if strings.HasPrefix(target, "PARTITION ") {
		alterTarget = target
	}
	return fmt.Errorf("effective gc.ttlseconds for table %s is %d (configured on %s), below the %d second (24 hour)"+
		" minimum: the initial snapshot reads AS OF SYSTEM TIME at one fixed timestamp, the changefeed resumes from"+
		" its last resolved timestamp, and both fail once that timestamp falls behind the replica GC threshold;"+
		" raise it with ALTER %s CONFIGURE ZONE USING gc.ttlseconds = <seconds>",
		table, ttlSeconds, target, gcTTLFloorSeconds, alterTarget)
}

// validateGCTTL checks that the MVCC GC window of every source table can
// cover the mirror's timestamp-pinned reads: the initial snapshot's AS OF
// SYSTEM TIME queries and the changefeed's resume cursor both fail once they
// fall behind the replica GC threshold, so a too-small window is caught here
// instead of mid-load. Mirrors with an initial snapshot additionally get a
// sizing warning with the estimated row count of the initial load.
func (c *CockroachDBConnector) validateGCTTL(
	ctx context.Context, tables []*common.QualifiedTable, doInitialSnapshot bool,
) error {
	minTTL := int64(-1)
	var minTTLTable, minTTLTarget string
	var totalRowEstimate int64
	for _, table := range tables {
		ttl, target, err := c.effectiveGCTTLSeconds(ctx, table)
		if err != nil {
			return fmt.Errorf("failed to determine the effective gc.ttlseconds of table %s"+
				" (SHOW ZONE CONFIGURATION and SHOW PARTITIONS need a grant on the table, e.g. SELECT): %w", table, err)
		}
		if err := validateGCTTLFloor(table.String(), target, ttl); err != nil {
			return err
		}
		if minTTL < 0 || ttl < minTTL {
			minTTL, minTTLTable, minTTLTarget = ttl, table.String(), target
		}
		if doInitialSnapshot {
			if estimate, err := c.tableRowEstimate(ctx, table); err != nil {
				c.logger.Warn("[cockroachdb] failed to estimate row count",
					slog.String("table", table.String()), slog.Any("error", err))
			} else {
				totalRowEstimate += estimate
			}
		}
	}
	if minTTL >= 0 && doInitialSnapshot {
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
// SHOW ZONE CONFIGURATION FOR TABLE returns the effective cascaded zone
// config with its origin (table, database or RANGE default) in the target
// column. Partition zone configs override the table level downward, so the
// minimum across the table and its partitions is the value that governs the
// mirror.
func (c *CockroachDBConnector) effectiveGCTTLSeconds(
	ctx context.Context, table *common.QualifiedTable,
) (int64, string, error) {
	var target, rawConfigSQL string
	if err := c.conn.QueryRow(ctx,
		"SHOW ZONE CONFIGURATION FOR TABLE "+table.String()).Scan(&target, &rawConfigSQL); err != nil {
		return 0, "", err
	}
	ttl, ok := parseGCTTLSeconds(rawConfigSQL)
	if !ok {
		return 0, "", fmt.Errorf("gc.ttlseconds not present in zone configuration for %s", target)
	}
	partitionTTL, partitionTarget, found, err := c.minPartitionGCTTLSeconds(ctx, table)
	if err != nil {
		return 0, "", err
	}
	if found && partitionTTL < ttl {
		return partitionTTL, partitionTarget, nil
	}
	return ttl, target, nil
}

// minPartitionGCTTLSeconds returns the smallest effective gc.ttlseconds among
// the partitions of a table, reported alongside a target naming the partition
// it came from. found is false for unpartitioned tables and for partitions
// without a zone config of their own (they inherit the table's).
func (c *CockroachDBConnector) minPartitionGCTTLSeconds(
	ctx context.Context, table *common.QualifiedTable,
) (int64, string, bool, error) {
	rows, err := c.conn.Query(ctx,
		"SELECT partition_name, zone_config FROM [SHOW PARTITIONS FROM TABLE "+table.String()+"]")
	if err != nil {
		return 0, "", false, fmt.Errorf("failed to list partitions of %s: %w", table, err)
	}
	defer rows.Close()
	minTTL := int64(-1)
	var minTarget string
	for rows.Next() {
		var partitionName, zoneConfig pgtype.Text
		if err := rows.Scan(&partitionName, &zoneConfig); err != nil {
			return 0, "", false, fmt.Errorf("failed to scan partitions of %s: %w", table, err)
		}
		if !zoneConfig.Valid {
			continue
		}
		if ttl, ok := parseGCTTLSeconds(zoneConfig.String); ok && (minTTL < 0 || ttl < minTTL) {
			minTTL = ttl
			minTarget = fmt.Sprintf("PARTITION %s OF TABLE %s", partitionName.String, table)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, "", false, fmt.Errorf("failed to list partitions of %s: %w", table, err)
	}
	return minTTL, minTarget, minTTL >= 0, nil
}

func (c *CockroachDBConnector) checkRangefeedEnabled(ctx context.Context) error {
	// crdb_internal.cluster_settings is gated behind allow_unsafe_internals on
	// v26.1+; SHOW CLUSTER SETTING only needs VIEWCLUSTERSETTING
	var value any
	if err := c.conn.QueryRow(ctx, "SHOW CLUSTER SETTING kv.rangefeed.enabled").Scan(&value); err != nil {
		return fmt.Errorf("failed to check the kv.rangefeed.enabled cluster setting, which changefeed based CDC"+
			" requires; reading it needs the VIEWCLUSTERSETTING or MODIFYCLUSTERSETTING privilege: %w", err)
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

// isUnknownSettingError reports whether err is CockroachDB rejecting a SHOW
// CLUSTER SETTING probe for a setting the cluster does not know about. That is
// the expected negative outcome when probing variant-specific settings, unlike
// network or privilege errors. CockroachDB raises these as "unknown setting:
// ..." with the uncategorized SQLSTATE XXUUU (verified on v25.4.13), so the
// message is the only reliable discriminator.
func isUnknownSettingError(err error) bool {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return strings.Contains(pgErr.Message, "unknown setting")
	}
	return false
}

// cockroachCloudHostSuffix marks hosts of managed CockroachDB Cloud clusters,
// letting variant detection classify them without querying the database.
const cockroachCloudHostSuffix = ".cockroachlabs.cloud"

func isCockroachCloudHost(host string) bool {
	return strings.HasSuffix(strings.ToLower(host), cockroachCloudHostSuffix)
}

func (c *CockroachDBConnector) GetDatabaseVariant(ctx context.Context) (protos.DatabaseVariant, error) {
	// managed CockroachDB Cloud hosts are recognizable by domain alone
	isCloud := isCockroachCloudHost(c.config.Host)

	if !isCloud {
		// SHOW CLUSTER SETTING works without allow_unsafe_internals, unlike
		// crdb_internal.cluster_settings. cluster.organization exists on every
		// CockroachDB flavor: self-hosted clusters return an empty string,
		// managed CockroachDB Cloud clusters always have it set. An
		// unknown-setting error is a definitive negative; anything else means
		// the probe itself failed (network, privileges) and is propagated so
		// the caller can retry instead of caching a wrong answer.
		var clusterOrg string
		if err := c.conn.QueryRow(ctx, "SHOW CLUSTER SETTING cluster.organization").Scan(&clusterOrg); err != nil {
			if isUnknownSettingError(err) {
				return protos.DatabaseVariant_VARIANT_UNKNOWN, nil
			}
			return protos.DatabaseVariant_VARIANT_UNKNOWN,
				fmt.Errorf("failed to probe cluster.organization for variant detection: %w", err)
		}
		isCloud = clusterOrg != ""
	}

	// server.serverless.enabled only exists on serverless hosts; everywhere
	// else the probe fails with an unknown-setting error, which is the
	// expected negative answer
	var serverless any
	if err := c.conn.QueryRow(ctx, "SHOW CLUSTER SETTING server.serverless.enabled").Scan(&serverless); err != nil {
		if !isUnknownSettingError(err) {
			return protos.DatabaseVariant_VARIANT_UNKNOWN,
				fmt.Errorf("failed to probe server.serverless.enabled for variant detection: %w", err)
		}
	} else if settingValueIsTrue(serverless) {
		return protos.DatabaseVariant_COCKROACHDB_SERVERLESS, nil
	}

	if isCloud {
		return protos.DatabaseVariant_COCKROACHDB_CLOUD, nil
	}

	return protos.DatabaseVariant_VARIANT_UNKNOWN, nil
}
