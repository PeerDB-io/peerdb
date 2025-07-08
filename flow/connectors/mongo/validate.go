package connmongo

import (
	"context"
	"errors"
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
		return err
	}

	serverStatus, err := shared_mongo.GetServerStatus(ctx, c.client)
	if err != nil {
		return err
	}
	if serverStatus.StorageEngine.Name != "wiredTiger" {
		return errors.New("storage engine must be 'wiredTiger'")
	}
	if serverStatus.OplogTruncation.OplogMinRetentionHours == 0 ||
		serverStatus.OplogTruncation.OplogMinRetentionHours < shared_mongo.MinOplogRetentionHours {
		return errors.New("oplog retention must be set to >= 24 hours")
	}

	return nil
}
