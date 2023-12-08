package main

import (
	"context"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
)

func (h *FlowRequestHandler) GetVersion(
	ctx context.Context,
	req *protos.PeerDBVersionRequest,
) (*protos.PeerDBVersionResponse, error) {
	version := utils.GetEnvString("PEERDB_VERSION_SHA_SHORT", "unknown")
	return &protos.PeerDBVersionResponse{Version: version}, nil
}
