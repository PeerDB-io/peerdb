package connmongo

import (
	"context"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	shared_mongo "github.com/PeerDB-io/peerdb/flow/pkg/mongo"
)

func (c *MongoConnector) ValidateCheck(ctx context.Context) error {
	if err := shared_mongo.ValidateUserRoles(ctx, c.client); err != nil {
		return err
	}

	if err := shared_mongo.ValidateServerCompatibility(ctx, c.client); err != nil {
		return err
	}

	return nil
}

func (c *MongoConnector) ValidateMirrorSource(ctx context.Context, cfg *protos.FlowConnectionConfigsCore) error {
	tables := make([]*common.QualifiedTable, 0, len(cfg.TableMappings))
	var deletePreimageTables []*common.QualifiedTable
	for _, tm := range cfg.TableMappings {
		t, err := common.ParseTableIdentifier(tm.SourceTableIdentifier)
		if err != nil {
			return err
		}
		tables = append(tables, t)
		if tm.MongoConfig.GetDeletePreimage() {
			deletePreimageTables = append(deletePreimageTables, t)
		}
	}
	if err := shared_mongo.ValidateCollections(ctx, c.client, tables); err != nil {
		return err
	}

	// no need to check oplog retention for initial-snapshot-only mirrors
	if cfg.DoInitialSnapshot && cfg.InitialSnapshotOnly {
		return nil
	}

	if len(deletePreimageTables) > 0 {
		if err := shared_mongo.ValidatePreAndPostImages(ctx, c.client, deletePreimageTables); err != nil {
			return err
		}
	}

	return shared_mongo.ValidateOplogRetention(ctx, c.client)
}
