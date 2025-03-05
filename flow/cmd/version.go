package cmd

import (
	"context"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

func (h *FlowRequestHandler) GetVersion(
	ctx context.Context,
	req *protos.PeerDBVersionRequest,
) (*protos.PeerDBVersionResponse, error) {
	version := internal.PeerDBVersionShaShort()
	return &protos.PeerDBVersionResponse{Version: version}, nil
}
