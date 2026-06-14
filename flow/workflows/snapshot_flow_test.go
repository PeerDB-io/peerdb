package peerflow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// Clone child workflow IDs must stay byte-identical to those built by
// pre-QualifiedTable releases (clone_<flow>_<dotted source>_<runID> with illegal
// characters replaced) so re-attached snapshot clones reuse the same IDs across an
// upgrade. The pre-refactor formula used the raw dotted source string.
func TestSnapshotCloneWorkflowIDMatchesLegacyFormat(t *testing.T) {
	const flowName = "my_flow"
	const runID = "0195cb9e-1a2b-7c3d-8e4f-aabbccddeeff"

	for _, tc := range []struct {
		legacyDotted string
		srcName      common.QualifiedTable
	}{
		{legacyDotted: "public.events", srcName: common.QualifiedTable{Namespace: "public", Table: "events"}},
		{legacyDotted: "sch.ema.ta.ble", srcName: common.QualifiedTable{Namespace: "sch.ema", Table: "ta.ble"}},
		{legacyDotted: "topic", srcName: common.QualifiedTable{Table: "topic"}},
	} {
		legacyID := shared.ReplaceIllegalCharactersWithUnderscores(
			fmt.Sprintf("clone_%s_%s_%s", flowName, tc.legacyDotted, runID))
		assert.Equal(t, legacyID, snapshotCloneWorkflowID(flowName, tc.srcName, runID),
			"clone workflow ID diverged from the pre-QualifiedTable format for %s", tc.legacyDotted)
	}
}
