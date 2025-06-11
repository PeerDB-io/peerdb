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
	versionResponse := protos.PeerDBVersionResponse{
		Version: internal.PeerDBVersionShaShort(),
	}
	if deploymentVersion := internal.PeerDBDeploymentVersion(); deploymentVersion != "" {
		versionResponse.DeploymentVersion = &deploymentVersion
	}
	return &versionResponse, nil
}
