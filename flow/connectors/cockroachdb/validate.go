package conncockroachdb

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
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
