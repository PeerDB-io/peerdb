package util

import (
	"github.com/PeerDB-io/peer-flow/shared"
	"go.temporal.io/sdk/log"
)

func FlowSignalHandler(activeSignal shared.CDCFlowSignal,
	v shared.CDCFlowSignal, logger log.Logger) shared.CDCFlowSignal {
	if v == shared.ShutdownSignal {
		logger.Info("received shutdown signal")
		return v
	} else if v == shared.PauseSignal {
		logger.Info("received pause signal")
		if activeSignal == shared.NoopSignal {
			logger.Info("workflow was running, pausing it")
			return v
		}
	} else if v == shared.NoopSignal {
		logger.Info("received resume signal")
		if activeSignal == shared.PauseSignal {
			logger.Info("workflow was paused, resuming it")
			return v
		}
	}
	return activeSignal
}
