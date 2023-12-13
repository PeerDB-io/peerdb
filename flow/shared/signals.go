package shared

import (
	"go.temporal.io/sdk/log"
)

func FlowSignalHandler(activeSignal CDCFlowSignal,
	v CDCFlowSignal, logger log.Logger) CDCFlowSignal {
	if v == ShutdownSignal {
		logger.Info("received shutdown signal")
		return v
	} else if v == PauseSignal {
		logger.Info("received pause signal")
		if activeSignal == NoopSignal {
			logger.Info("workflow was running, pausing it")
			return v
		}
	} else if v == NoopSignal {
		logger.Info("received resume signal")
		if activeSignal == PauseSignal {
			logger.Info("workflow was paused, resuming it")
			return v
		}
	}
	return activeSignal
}
