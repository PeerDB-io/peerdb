package shared

import (
	"context"
	"fmt"

	"github.com/jackc/pgerrcode"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func GetWorkflowID(flowName string) string {
	return flowName + "-peerflow"
}

// CreateCdcJobEntry creates a CDC job entry in the flows table
func CreateCdcJobEntry(
	ctx context.Context,
	pool CatalogPool,
	connectionConfigs *protos.FlowConnectionConfigsCore,
	workflowID string,
	idempotent bool,
) error {
	sourcePeerID, srcErr := GetPeerID(ctx, pool, connectionConfigs.SourceName)
	if srcErr != nil {
		return fmt.Errorf("unable to get peer id for source peer %s: %w",
			connectionConfigs.SourceName, srcErr)
	}

	destinationPeerID, dstErr := GetPeerID(ctx, pool, connectionConfigs.DestinationName)
	if dstErr != nil {
		return fmt.Errorf("unable to get peer id for target peer %s: %w",
			connectionConfigs.DestinationName, dstErr)
	}

	cfgBytes, err := proto.Marshal(connectionConfigs)
	if err != nil {
		return fmt.Errorf("unable to marshal flow config: %w", err)
	}

	if _, err = pool.Exec(ctx,
		`INSERT INTO flows (workflow_id, name, source_peer, destination_peer, config_proto, status, description)
        VALUES ($1,$2,$3,$4,$5,$6,'gRPC')`,
		workflowID, connectionConfigs.FlowJobName, sourcePeerID, destinationPeerID, cfgBytes, protos.FlowStatus_STATUS_SETUP,
	); err != nil && !(idempotent && IsSQLStateError(err, pgerrcode.UniqueViolation)) {
		return fmt.Errorf("unable to insert into flows table for flow %s: %w",
			connectionConfigs.FlowJobName, err)
	}

	return nil
}

// GetPeerID retrieves the peer ID for a given peer name
func GetPeerID(ctx context.Context, pool CatalogPool, peerName string) (int32, error) {
	var peerID int32
	err := pool.QueryRow(ctx,
		"SELECT id FROM peers WHERE name = $1",
		peerName).Scan(&peerID)
	if err != nil {
		return 0, fmt.Errorf("failed to get peer ID for %s: %w", peerName, err)
	}
	return peerID, nil
}
