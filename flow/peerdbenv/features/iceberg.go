package features

import (
	"context"
	"log/slog"
	"strconv"

	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

func IcebergFeatureStreamingDisabled(ctx context.Context) bool {
	strValue := peerdbenv.GetEnvString("ICEBERG_FEATURE_STREAMING_DISABLED", "false")
	value, err := strconv.ParseBool(strValue)
	if err != nil {
		// only log and return false
		logger.LoggerFromCtx(ctx).Error("Failed to get ICEBERG_FEATURE_STREAMING_DISABLED", slog.Any("error", err))
		return false
	}
	return value
}
