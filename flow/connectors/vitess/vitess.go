package connvitess

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/log"
	"vitess.io/vitess/go/vt/proto/query"
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	metadataStore "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

type VitessConnector struct {
	*metadataStore.PostgresMetadata
	ssh    utils.SSHTunnel
	logger log.Logger
}

func NewVitessConnector(ctx context.Context, config *protos.VitessConfig) (*VitessConnector, error) {
	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		return nil, err
	}
	ssh, err := utils.NewSSHTunnel(ctx, config.SshConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create ssh tunnel: %w", err)
	}
	logger := internal.LoggerFromCtx(ctx)

	return &VitessConnector{
		PostgresMetadata: pgMetadata,
		ssh:              ssh,
		logger:           logger,
	}, nil
}

func (c *VitessConnector) Execute(ctx context.Context, query string) (*query.ExecuteResponse, error) {
	return nil, nil
}
