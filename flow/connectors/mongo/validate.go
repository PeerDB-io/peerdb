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
	if cfg.DoInitialSnapshot && cfg.InitialSnapshotOnly {
		return nil
	}

	if err := shared_mongo.ValidateOplogRetention(ctx, c.client); err != nil {
		return err
	}

	tables := make([]*common.QualifiedTable, 0, len(cfg.TableMappings))
	for _, tm := range cfg.TableMappings {
		t, err := common.ParseTableIdentifier(tm.SourceTableIdentifier)
		if err != nil {
			return err
		}
		tables = append(tables, t)
	}
	if err := shared_mongo.ValidateCollections(ctx, c.client, tables); err != nil {
		return err
	}

	return nil
}
