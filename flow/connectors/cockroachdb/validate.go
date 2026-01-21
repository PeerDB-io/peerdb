package conncockroachdb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

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
	for _, tm := range cfg.TableMappings {
		parsedTable, err := common.ParseTableIdentifier(tm.SourceTableIdentifier)
		if err != nil {
			return fmt.Errorf("invalid source table identifier %s: %w", tm.SourceTableIdentifier, err)
		}
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

func (c *CockroachDBConnector) checkRangefeedEnabled(ctx context.Context) error {
	var value string
	if err := c.conn.QueryRow(ctx,
		"SELECT value FROM crdb_internal.cluster_settings WHERE variable = 'kv.rangefeed.enabled'",
	).Scan(&value); err != nil {
		// insufficient privileges to read cluster settings shouldn't block
		// mirror creation; changefeed creation surfaces the error otherwise
		c.logger.Warn("[cockroachdb] unable to check kv.rangefeed.enabled", slog.Any("error", err))
		return nil
	}
	if value != "true" {
		return errors.New("changefeeds require rangefeeds: run SET CLUSTER SETTING kv.rangefeed.enabled = true;" +
			" rangefeeds are enabled by default on CockroachDB Cloud, self-hosted clusters must enable them explicitly")
	}
	return nil
}

func (c *CockroachDBConnector) GetDatabaseVariant(ctx context.Context) (protos.DatabaseVariant, error) {
	// Query to determine CockroachDB deployment type
	var clusterOrg string
	err := c.conn.QueryRow(ctx, `
		SELECT value FROM crdb_internal.cluster_settings 
		WHERE variable = 'cluster.organization'
	`).Scan(&clusterOrg)
	if err != nil {
		// If we can't determine, return unknown
		return protos.DatabaseVariant_VARIANT_UNKNOWN, nil
	}

	// Check for serverless tier
	var isServerless bool
	err = c.conn.QueryRow(ctx, `
		SELECT value::BOOL FROM crdb_internal.cluster_settings 
		WHERE variable = 'server.serverless.enabled'
	`).Scan(&isServerless)
	if err == nil && isServerless {
		return protos.DatabaseVariant_COCKROACHDB_SERVERLESS, nil
	}

	// Check if it's CockroachDB Cloud by looking at organization
	if clusterOrg != "" {
		return protos.DatabaseVariant_COCKROACHDB_CLOUD, nil
	}

	return protos.DatabaseVariant_VARIANT_UNKNOWN, nil
}
