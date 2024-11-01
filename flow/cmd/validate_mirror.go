package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/PeerDB-io/peer-flow/connectors"
	connclickhouse "github.com/PeerDB-io/peer-flow/connectors/clickhouse"
	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared/telemetry"
)

var (
	CustomColumnTypeRegex = regexp.MustCompile(`^$|^[a-zA-Z][a-zA-Z0-9(),]*$`)
	CustomColumnNameRegex = regexp.MustCompile(`^$|^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

func (h *FlowRequestHandler) ValidateCDCMirror(
	ctx context.Context, req *protos.CreateCDCFlowRequest,
) (*protos.ValidateCDCMirrorResponse, error) {
	underMaintenance, err := peerdbenv.PeerDBMaintenanceModeEnabled(ctx, nil)
	if err != nil {
		slog.Error("unable to check maintenance mode", slog.Any("error", err))
		return nil, fmt.Errorf("unable to load dynamic config: %w", err)
	}

	if underMaintenance {
		slog.Warn("Validate request denied due to maintenance", "flowName", req.ConnectionConfigs.FlowJobName)
		return nil, errors.New("PeerDB is under maintenance")
	}

	if !req.ConnectionConfigs.Resync {
		mirrorExists, existCheckErr := h.CheckIfMirrorNameExists(ctx, req.ConnectionConfigs.FlowJobName)
		if existCheckErr != nil {
			slog.Error("/validatecdc failed to check if mirror name exists", slog.Any("error", existCheckErr))
			return nil, existCheckErr
		}

		if mirrorExists {
			displayErr := fmt.Errorf("mirror with name %s already exists", req.ConnectionConfigs.FlowJobName)
			h.alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, req.ConnectionConfigs.FlowJobName, displayErr.Error())
			return nil, displayErr
		}
	}

	if req.ConnectionConfigs == nil {
		slog.Error("/validatecdc connection configs is nil")
		return nil, errors.New("connection configs is nil")
	}
	sourcePeer, err := connectors.LoadPeer(ctx, h.pool, req.ConnectionConfigs.SourceName)
	if err != nil {
		slog.Error("/validatecdc failed to load source peer", slog.String("peer", req.ConnectionConfigs.SourceName))
		return nil, err
	}

	sourcePeerConfig := sourcePeer.GetPostgresConfig()
	if sourcePeerConfig == nil {
		slog.Error("/validatecdc source peer config is not postgres", slog.String("peer", req.ConnectionConfigs.SourceName))
		return nil, errors.New("source peer config is not postgres")
	}

	pgPeer, err := connpostgres.NewPostgresConnector(ctx, nil, sourcePeerConfig)
	if err != nil {
		displayErr := fmt.Errorf("failed to create postgres connector: %v", err)
		h.alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, req.ConnectionConfigs.FlowJobName, displayErr.Error())
		return nil, displayErr
	}
	defer pgPeer.Close()

	noCDC := req.ConnectionConfigs.DoInitialSnapshot && req.ConnectionConfigs.InitialSnapshotOnly
	if !noCDC {
		// Check replication connectivity
		if err := pgPeer.CheckReplicationConnectivity(ctx); err != nil {
			displayErr := fmt.Errorf("unable to establish replication connectivity: %v", err)
			h.alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, req.ConnectionConfigs.FlowJobName,
				displayErr.Error(),
			)
			return nil, displayErr
		}

		// Check permissions of postgres peer
		if err := pgPeer.CheckReplicationPermissions(ctx, sourcePeerConfig.User); err != nil {
			displayErr := fmt.Errorf("failed to check replication permissions: %v", err)
			h.alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, req.ConnectionConfigs.FlowJobName, displayErr.Error())
			return nil, displayErr
		}
	}

	sourceTables := make([]*utils.SchemaTable, 0, len(req.ConnectionConfigs.TableMappings))
	srcTableNames := make([]string, 0, len(req.ConnectionConfigs.TableMappings))
	for _, tableMapping := range req.ConnectionConfigs.TableMappings {
		parsedTable, parseErr := utils.ParseSchemaTable(tableMapping.SourceTableIdentifier)
		if parseErr != nil {
			displayErr := fmt.Errorf("invalid source table identifier: %s", parseErr)
			h.alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, req.ConnectionConfigs.FlowJobName, displayErr.Error())
			return nil, displayErr
		}

		sourceTables = append(sourceTables, parsedTable)
		srcTableNames = append(srcTableNames, tableMapping.SourceTableIdentifier)
	}

	pubName := req.ConnectionConfigs.PublicationName

	if pubName == "" && !noCDC {
		srcTableNames := make([]string, 0, len(sourceTables))
		for _, srcTable := range sourceTables {
			srcTableNames = append(srcTableNames, fmt.Sprintf(`%s.%s`,
				connpostgres.QuoteIdentifier(srcTable.Schema),
				connpostgres.QuoteIdentifier(srcTable.Table)),
			)
		}

		if err := pgPeer.CheckPublicationCreationPermissions(ctx, srcTableNames); err != nil {
			displayErr := fmt.Errorf("invalid publication creation permissions: %v", err)
			h.alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, req.ConnectionConfigs.FlowJobName, displayErr.Error())
			return nil, displayErr
		}
	}

	if err := pgPeer.CheckSourceTables(ctx, sourceTables, pubName, noCDC); err != nil {
		displayErr := fmt.Errorf("provided source tables invalidated: %v", err)
		slog.Error(displayErr.Error())
		h.alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, req.ConnectionConfigs.FlowJobName, displayErr.Error())
		return nil, displayErr
	}

	for _, tm := range req.ConnectionConfigs.TableMappings {
		for _, col := range tm.Columns {
			if !CustomColumnTypeRegex.MatchString(col.DestinationType) {
				return nil, fmt.Errorf("invalid custom column type %s", col.DestinationType)
			}
			if !CustomColumnNameRegex.MatchString(col.DestinationName) {
				return nil, fmt.Errorf("invalid custom column name %s", col.DestinationName)
			}
		}
	}

	dstPeer, err := connectors.LoadPeer(ctx, h.pool, req.ConnectionConfigs.DestinationName)
	if err != nil {
		slog.Error("/validatecdc failed to load destination peer", slog.String("peer", req.ConnectionConfigs.DestinationName))
		return nil, err
	}
	if dstPeer.GetClickhouseConfig() != nil {
		chPeer, err := connclickhouse.NewClickHouseConnector(ctx, nil, dstPeer.GetClickhouseConfig())
		if err != nil {
			displayErr := fmt.Errorf("failed to create clickhouse connector: %w", err)
			h.alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, req.ConnectionConfigs.FlowJobName,
				displayErr.Error(),
			)
			return nil, displayErr
		}
		defer chPeer.Close()

		res, err := pgPeer.GetTableSchema(ctx, nil, req.ConnectionConfigs.System, srcTableNames)
		if err != nil {
			displayErr := fmt.Errorf("failed to get source table schema: %v", err)
			h.alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, req.ConnectionConfigs.FlowJobName,
				displayErr.Error(),
			)
			return nil, displayErr
		}

		err = chPeer.CheckDestinationTables(ctx, req.ConnectionConfigs, res)
		if err != nil {
			h.alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, req.ConnectionConfigs.FlowJobName,
				err.Error(),
			)
			return nil, err
		}
	}

	return &protos.ValidateCDCMirrorResponse{}, nil
}

func (h *FlowRequestHandler) CheckIfMirrorNameExists(ctx context.Context, mirrorName string) (bool, error) {
	var nameExists pgtype.Bool
	err := h.pool.QueryRow(ctx, "SELECT EXISTS(SELECT * FROM flows WHERE name = $1)", mirrorName).Scan(&nameExists)
	if err != nil {
		return false, fmt.Errorf("failed to check if mirror name exists: %v", err)
	}

	return nameExists.Bool, nil
}
