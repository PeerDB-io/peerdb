package mirrorutils

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

func GetPeerID(ctx context.Context, catalogPool *pgxpool.Pool, peerName string) (int32, int32, error) {
	var id pgtype.Int4
	var peerType pgtype.Int4
	err := catalogPool.QueryRow(ctx, "SELECT id,type FROM peers WHERE name = $1", peerName).Scan(&id, &peerType)
	if err != nil {
		slog.Error("unable to query peer id for peer "+peerName, slog.Any("error", err))
		return -1, -1, fmt.Errorf("unable to query peer id for peer %s: %s", peerName, err)
	}
	return id.Int32, peerType.Int32, nil
}

func schemaForTableIdentifier(tableIdentifier string, peerDBType int32) string {
	if peerDBType != int32(protos.DBType_BIGQUERY) && !strings.ContainsRune(tableIdentifier, '.') {
		return "public." + tableIdentifier
	}
	return tableIdentifier
}

func CreateCDCWorkflowEntry(ctx context.Context, catalogPool *pgxpool.Pool,
	cfg *protos.FlowConnectionConfigs, workflowID string,
) error {
	sourcePeerID, sourcePeerType, srcErr := GetPeerID(ctx, catalogPool, cfg.SourceName)
	if srcErr != nil {
		return fmt.Errorf("unable to get peer id for source peer %s: %w",
			cfg.SourceName, srcErr)
	}

	destinationPeerID, destinationPeerType, dstErr := GetPeerID(ctx, catalogPool, cfg.DestinationName)
	if dstErr != nil {
		return fmt.Errorf("unable to get peer id for target peer %s: %w",
			cfg.DestinationName, srcErr)
	}

	for _, v := range cfg.TableMappings {
		_, err := catalogPool.Exec(ctx, `
		INSERT INTO flows (workflow_id, name, source_peer, destination_peer, description,
		source_table_identifier, destination_table_identifier) VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, workflowID, cfg.FlowJobName, sourcePeerID, destinationPeerID,
			"Mirror created via GRPC",
			schemaForTableIdentifier(v.SourceTableIdentifier, sourcePeerType),
			schemaForTableIdentifier(v.DestinationTableIdentifier, destinationPeerType))
		if err != nil {
			return fmt.Errorf("unable to insert into flows table for flow %s with source table %s: %w",
				cfg.FlowJobName, v.SourceTableIdentifier, err)
		}
	}

	return nil
}
