package connmysql

import (
	"context"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func (c *MySqlConnector) ValidateMirrorSource(ctx context.Context, cfg *protos.FlowConnectionConfigs) error {
	return nil
}
