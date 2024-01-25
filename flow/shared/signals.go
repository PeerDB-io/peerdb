package shared

import (
	"go.temporal.io/sdk/log"
)

func FlowSignalHandler(activeSignal CDCFlowSignal,
	v CDCFlowSignal, logger log.Logger,
) CDCFlowSignal {
	switch v {
	case PauseSignal:
		logger.Info("received pause signal")
		if activeSignal == NoopSignal {
			logger.Info("workflow was running, pausing it")
			return v
		}
	case NoopSignal:
		logger.Info("received resume signal")
		if activeSignal == PauseSignal {
			logger.Info("workflow was paused, resuming it")
			return v
		}
	}
	return activeSignal
}
