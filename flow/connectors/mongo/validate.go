package connmongo

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	shared_mongo "github.com/PeerDB-io/peerdb/flow/shared/mongo"
)

func (c *MongoConnector) ValidateCheck(ctx context.Context) error {
	version, err := c.GetVersion(ctx)
	if err != nil {
		return err
	}
	cmp, err := shared_mongo.CompareServerVersions(version, shared_mongo.MinSupportedVersion)
	if err != nil {
		return err
	}
	if cmp == -1 {
		return fmt.Errorf("require minimum mongo version %s", shared_mongo.MinSupportedVersion)
	}
	return nil
}

func (c *MongoConnector) ValidateMirrorSource(ctx context.Context, cfg *protos.FlowConnectionConfigs) error {
	if cfg.DoInitialSnapshot && cfg.InitialSnapshotOnly {
		return nil
	}

	_, err := shared_mongo.GetReplSetGetStatus(ctx, c.client)
	if err != nil {
		return fmt.Errorf("failed to get replica set status: %w", err)
	}
	return nil
}
