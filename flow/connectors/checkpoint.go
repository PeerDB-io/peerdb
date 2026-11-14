package connectors

import (
	"context"
	"errors"
	"fmt"

	connmetadata "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	connmysql "github.com/PeerDB-io/peerdb/flow/connectors/mysql"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

var ErrInvalidCheckpointReset = errors.New("invalid CDC checkpoint reset")

// ValidateCdcCheckpointReset checks that checkpoint can replace the stored CDC checkpoint
// of the flow and returns the current checkpoint text.
// Errors caused by the requested checkpoint wrap ErrInvalidCheckpointReset.
func ValidateCdcCheckpointReset(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	flowName string,
	sourceName string,
	checkpoint string,
) (string, error) {
	srcPeer, err := LoadPeer(ctx, catalogPool, sourceName)
	if err != nil {
		return "", fmt.Errorf("failed to load source peer: %w", err)
	}
	mysqlConfig := srcPeer.GetMysqlConfig()
	if mysqlConfig == nil {
		return "", fmt.Errorf("%w: only supported for MySQL sources, got %s", ErrInvalidCheckpointReset, srcPeer.Type)
	}
	current, err := connmetadata.NewPostgresMetadataFromCatalog(internal.LoggerFromCtx(ctx), catalogPool).
		GetLastOffset(ctx, flowName)
	if err != nil {
		return "", fmt.Errorf("failed to get current checkpoint: %w", err)
	}
	if err := connmysql.ValidateCheckpointReset(mysqlConfig.Flavor, current.Text, checkpoint); err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidCheckpointReset, err)
	}
	return current.Text, nil
}
