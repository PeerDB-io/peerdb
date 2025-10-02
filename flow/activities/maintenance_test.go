package activities

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestMaintenanceStatusesAreCorrectlyPopulated(t *testing.T) {
	statuses := buildWaitStatuses()
	t.Logf("statuses: %v", statuses)
	assert.Contains(t, statuses, protos.FlowStatus_STATUS_SNAPSHOT, "Snapshot should be in statuses")
	assert.NotContains(t, statuses, protos.FlowStatus_STATUS_RUNNING, "Running should not be in statuses")
}
