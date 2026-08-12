package peerflow

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/worker"
)

// TestCDCFlowWorkflowReplay replays recorded CDCFlowWorkflow histories against the
// current workflow code. A failure here means the current code produces a different
// command sequence than the code that recorded the history, which would cause
// TMPRL1100 non-determinism errors on live workflows after a deploy.
//
// The fixtures that the test runs against reside in testdata/replay/. Since we don't
// employ patching or versioning today; the event history should always replay successfully.
//
// If this test fails after an intentional change, the change may NOT be safe to deploy
// while any mirror has a run recorded by the previous release.
func TestCDCFlowWorkflowReplay(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("testdata", "replay", "*.json"))
	require.NoError(t, err)
	require.NotEmpty(t, files,
		"no replay fixtures found under testdata/replay; regenerate with the instructions in cdc_flow_replay_fixtures_test.go")

	for _, file := range files {
		t.Run(filepath.Base(file), func(t *testing.T) {
			replayer := worker.NewWorkflowReplayer()
			replayer.RegisterWorkflow(CDCFlowWorkflow)
			require.NoError(t, replayer.ReplayWorkflowHistoryFromJSONFile(nil, file),
				"replay diverged from recorded history: this change breaks determinism for in-flight CDCFlowWorkflow runs")
		})
	}
}
