package peerflow

import (
	"github.com/PeerDB-io/peer-flow/activities"
)

const (
	FlowStatusQuery  = "q-flow-status"
	FlowStatusUpdate = "u-flow-status"
)

var (
	flowable *activities.FlowableActivity
	snapshot *activities.SnapshotActivity
)
