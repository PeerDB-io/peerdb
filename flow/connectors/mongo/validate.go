package connmongo

import (
	"context"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	shared_mongo "github.com/PeerDB-io/peerdb/flow/shared/mongo"
)

func (c *MongoConnector) ValidateCheck(ctx context.Context) error {
	if err := shared_mongo.ValidateServerCompatibility(ctx, c.client, shared_mongo.Credentials{
		Username:   c.config.Username,
		Password:   c.config.Password,
		DisableTls: c.config.DisableTls,
		RootCa:     c.config.RootCa,
		TlsHost:    c.config.TlsHost,
	}); err != nil {
		return err
	}

	if err := shared_mongo.ValidateUserRoles(ctx, c.client); err != nil {
		return err
	}

	return nil
}

func (c *MongoConnector) ValidateMirrorSource(ctx context.Context, cfg *protos.FlowConnectionConfigs) error {
	if cfg.DoInitialSnapshot && cfg.InitialSnapshotOnly {
		return nil
	}

	if err := shared_mongo.ValidateOplogRetention(ctx, c.client, shared_mongo.Credentials{
		Username:   c.config.Username,
		Password:   c.config.Password,
		DisableTls: c.config.DisableTls,
		RootCa:     c.config.RootCa,
		TlsHost:    c.config.TlsHost,
	}); err != nil {
		return err
	}

	return nil
}
