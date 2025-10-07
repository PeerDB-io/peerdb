package internal

import "github.com/PeerDB-io/peerdb/flow/generated/protos"

func MinimizeFlowConfiguration(cfg *protos.FlowConnectionConfigs) *protos.FlowConnectionConfigs {
	if cfg == nil {
		return nil
	}
	return &protos.FlowConnectionConfigs{
		FlowJobName: cfg.FlowJobName,
	}
}
