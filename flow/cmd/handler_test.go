package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestTerminationSignalRequest(t *testing.T) {
	t.Run("terminated is signaled as terminating", func(t *testing.T) {
		req := &protos.FlowStateChangeRequest{
			FlowJobName:         "m",
			RequestedFlowState:  protos.FlowStatus_STATUS_TERMINATED,
			DropMirrorStats:     true,
			SkipDestinationDrop: true,
		}
		signalReq := terminationSignalRequest(req)
		require.Equal(t, protos.FlowStatus_STATUS_TERMINATING, signalReq.RequestedFlowState)
		require.Equal(t, "m", signalReq.FlowJobName)
		require.True(t, signalReq.DropMirrorStats)
		require.True(t, signalReq.SkipDestinationDrop)
		require.Equal(t, protos.FlowStatus_STATUS_TERMINATED, req.RequestedFlowState)
	})

	t.Run("terminating is unchanged", func(t *testing.T) {
		req := &protos.FlowStateChangeRequest{
			FlowJobName:        "m",
			RequestedFlowState: protos.FlowStatus_STATUS_TERMINATING,
		}
		require.Same(t, req, terminationSignalRequest(req))
	})
}
