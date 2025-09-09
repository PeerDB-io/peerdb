package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connclickhouse "github.com/PeerDB-io/peerdb/flow/connectors/clickhouse"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func redactProto(message proto.Message) {
	message.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Kind() == protoreflect.MessageKind {
			redactProto(v.Message().Interface())
		} else if fd.Kind() == protoreflect.StringKind {
			redacted := proto.GetExtension(fd.Options().(*descriptorpb.FieldOptions), protos.E_PeerdbRedacted).(bool)
			if redacted {
				message.ProtoReflect().Set(fd, protoreflect.ValueOfString("********"))
			}
		}
		return true
	})
}

func (h *FlowRequestHandler) GetPeerInfo(
	ctx context.Context,
	req *protos.PeerInfoRequest,
) (*protos.PeerInfoResponse, error) {
	ctx, cancelCtx := context.WithTimeout(ctx, 30*time.Second)
	defer cancelCtx()
	peer, err := connectors.LoadPeer(ctx, h.pool, req.PeerName)
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, exceptions.NewInternalApiError(fmt.Sprintf("failed to load peer: %v", err))
	}

	var version string
	versionConnector, err := connectors.GetAs[connectors.GetVersionConnector](ctx, nil, peer)
	if err != nil {
		if !errors.Is(err, errors.ErrUnsupported) {
			slog.Error("failed to get version connector", slog.Any("error", err))
		}
	} else {
		defer connectors.CloseConnector(ctx, versionConnector)
		version, err = versionConnector.GetVersion(ctx)
		if err != nil {
			slog.Error("failed to get version", slog.Any("error", err))
		}
	}

	peer.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Kind() == protoreflect.MessageKind {
			redactProto(v.Message().Interface())
		}
		return true
	})

	return &protos.PeerInfoResponse{
		Peer:    peer,
		Version: version,
	}, nil
}

func (h *FlowRequestHandler) GetPeerType(
	ctx context.Context,
	req *protos.PeerInfoRequest,
) (*protos.PeerTypeResponse, error) {
	ctx, cancelCtx := context.WithTimeout(ctx, 30*time.Second)
	defer cancelCtx()
	peer, err := connectors.LoadPeer(ctx, h.pool, req.PeerName)
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, exceptions.NewInternalApiError(fmt.Sprintf("failed to load peer: %v", err))
	}

	return &protos.PeerTypeResponse{
		PeerType: peer.Type.String(),
	}, nil
}

func (h *FlowRequestHandler) ListPeers(
	ctx context.Context,
	req *protos.ListPeersRequest,
) (*protos.ListPeersResponse, error) {
	query := "SELECT name, type FROM peers"
	if internal.PeerDBOnlyClickHouseAllowed() {
		// only postgres, mysql, mongo,and clickhouse
		query += fmt.Sprintf(" WHERE type IN (%d,%d,%d,%d)",
			protos.DBType_POSTGRES, protos.DBType_MYSQL, protos.DBType_MONGO, protos.DBType_CLICKHOUSE)
	}
	rows, err := h.pool.Query(ctx, query)
	if err != nil {
		slog.Error("failed to query for peers", slog.Any("error", err))
		return nil, exceptions.NewInternalApiError(fmt.Sprintf("failed to query for peers: %v", err))
	}
	peers, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.PeerListItem, error) {
		var peer protos.PeerListItem
		return &peer, row.Scan(&peer.Name, &peer.Type)
	})
	if err != nil {
		slog.Error("failed to collect peers", slog.Any("error", err))
		return nil, exceptions.NewInternalApiError(fmt.Sprintf("failed to collect peers: %v", err))
	}

	sourceItems := make([]*protos.PeerListItem, 0, len(peers))
	destinationItems := make([]*protos.PeerListItem, 0, len(peers))
	for _, peer := range peers {
		if peer.Type == protos.DBType_POSTGRES || peer.Type == protos.DBType_MYSQL || peer.Type == protos.DBType_MONGO {
			sourceItems = append(sourceItems, peer)
		}
		if peer.Type != protos.DBType_MYSQL &&
			peer.Type != protos.DBType_MONGO && (!internal.PeerDBOnlyClickHouseAllowed() || peer.Type == protos.DBType_CLICKHOUSE) {
			destinationItems = append(destinationItems, peer)
		}
	}

	return &protos.ListPeersResponse{
		Items:            peers,
		SourceItems:      sourceItems,
		DestinationItems: destinationItems,
	}, nil
}

func (h *FlowRequestHandler) GetSchemas(
	ctx context.Context,
	req *protos.PostgresPeerActivityInfoRequest,
) (*protos.PeerSchemasResponse, error) {
	conn, err := connectors.GetByNameAs[connectors.GetSchemaConnector](ctx, nil, h.pool, req.PeerName)
	if err != nil {
		return nil, exceptions.NewInvalidArgumentApiError(fmt.Sprintf("failed to get schema connector: %v", err))
	}
	defer connectors.CloseConnector(ctx, conn)
	return conn.GetSchemas(ctx)
}

func (h *FlowRequestHandler) GetTablesInSchema(
	ctx context.Context,
	req *protos.SchemaTablesRequest,
) (*protos.SchemaTablesResponse, error) {
	conn, err := connectors.GetByNameAs[connectors.GetSchemaConnector](ctx, nil, h.pool, req.PeerName)
	if err != nil {
		return nil, exceptions.NewFailedPreconditionApiError(fmt.Sprintf("failed to get schema connector: %v", err))
	}
	defer connectors.CloseConnector(ctx, conn)
	return conn.GetTablesInSchema(ctx, req.SchemaName, req.CdcEnabled)
}

// Returns list of tables across schema in schema.table format
func (h *FlowRequestHandler) GetAllTables(
	ctx context.Context,
	req *protos.PostgresPeerActivityInfoRequest,
) (*protos.AllTablesResponse, error) {
	conn, err := connectors.GetByNameAs[connectors.GetSchemaConnector](ctx, nil, h.pool, req.PeerName)
	if err != nil {
		return nil, exceptions.NewFailedPreconditionApiError(fmt.Sprintf("failed to get schema connector: %v", err))
	}
	defer connectors.CloseConnector(ctx, conn)
	return conn.GetAllTables(ctx)
}

func (h *FlowRequestHandler) GetColumns(
	ctx context.Context,
	req *protos.TableColumnsRequest,
) (*protos.TableColumnsResponse, error) {
	conn, err := connectors.GetByNameAs[connectors.GetSchemaConnector](ctx, nil, h.pool, req.PeerName)
	if err != nil {
		return nil, exceptions.NewFailedPreconditionApiError(fmt.Sprintf("failed to get schema connector: %v", err))
	}
	defer connectors.CloseConnector(ctx, conn)
	internalVersion, err := internal.PeerDBForceInternalVersion(ctx, nil)
	if err != nil {
		return nil, exceptions.NewInternalApiError(fmt.Sprintf("failed to get internal version: %v", err))
	}
	return conn.GetColumns(ctx, internalVersion, req.SchemaName, req.TableName)
}

func (h *FlowRequestHandler) GetColumnsTypeConversion(
	ctx context.Context,
	req *protos.ColumnsTypeConversionRequest,
) (*protos.ColumnsTypeConversionResponse, error) {
	return connclickhouse.GetColumnsTypeConversion()
}

func (h *FlowRequestHandler) GetSlotInfo(
	ctx context.Context,
	req *protos.PostgresPeerActivityInfoRequest,
) (*protos.PeerSlotResponse, error) {
	pgConnector, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, nil, h.pool, req.PeerName)
	if err != nil {
		slog.Error("Failed to create postgres connector", slog.Any("error", err))
		return nil, exceptions.NewFailedPreconditionApiError(fmt.Sprintf("failed to get postgres connector: %v", err))
	}
	defer connectors.CloseConnector(ctx, pgConnector)

	slotInfo, err := pgConnector.GetSlotInfo(ctx, "")
	if err != nil {
		slog.Error("Failed to get slot info", slog.Any("error", err))
		return nil, exceptions.NewFailedPreconditionApiError(fmt.Sprintf("failed to get slot info: %v", err))
	}

	return &protos.PeerSlotResponse{
		SlotData: slotInfo,
	}, nil
}

func (h *FlowRequestHandler) GetSlotLagHistory(
	ctx context.Context,
	req *protos.GetSlotLagHistoryRequest,
) (*protos.GetSlotLagHistoryResponse, error) {
	rows, err := h.pool.Query(ctx, `select updated_at, slot_size,
			coalesce(redo_lsn,''), coalesce(restart_lsn,''), coalesce(confirmed_flush_lsn,'')
		from peerdb_stats.peer_slot_size
		where slot_size is not null
			and peer_name = $1
			and slot_name = $2
			and updated_at > (now()-$3::INTERVAL)
		order by random() limit 720`, req.PeerName, req.SlotName, req.TimeSince)
	if err != nil {
		return nil, exceptions.NewInternalApiError(fmt.Sprintf("failed to get slot lag history: %v", err))
	}
	points, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.SlotLagPoint, error) {
		var updatedAt time.Time
		var slotSize int64
		var redoLSN string
		var restartLSN string
		var confirmedFlushLSN string
		if err := row.Scan(&updatedAt, &slotSize, &redoLSN, &restartLSN, &confirmedFlushLSN); err != nil {
			return nil, err
		}
		return &protos.SlotLagPoint{
			Time:         float64(updatedAt.UnixMilli()),
			Size:         float64(slotSize) / 1000.0,
			RedoLSN:      redoLSN,
			RestartLSN:   restartLSN,
			ConfirmedLSN: confirmedFlushLSN,
		}, nil
	})
	if err != nil {
		return nil, exceptions.NewInternalApiError(fmt.Sprintf("failed to collect slot lag history: %v", err))
	}

	return &protos.GetSlotLagHistoryResponse{Data: points}, nil
}

func (h *FlowRequestHandler) GetStatInfo(
	ctx context.Context,
	req *protos.PostgresPeerActivityInfoRequest,
) (*protos.PeerStatResponse, error) {
	peerConn, err := connectors.GetByNameAs[connectors.StatActivityConnector](ctx, nil, h.pool, req.PeerName)
	if err != nil {
		return nil, exceptions.NewFailedPreconditionApiError(fmt.Sprintf("failed to get stat activity connector: %v", err))
	}
	defer connectors.CloseConnector(ctx, peerConn)

	return peerConn.StatActivity(ctx, req)
}

func (h *FlowRequestHandler) GetPublications(
	ctx context.Context,
	req *protos.PostgresPeerActivityInfoRequest,
) (*protos.PeerPublicationsResponse, error) {
	peerConn, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, nil, h.pool, req.PeerName)
	if err != nil {
		return nil, exceptions.NewFailedPreconditionApiError(fmt.Sprintf("failed to get postgres connector: %v", err))
	}
	defer connectors.CloseConnector(ctx, peerConn)

	rows, err := peerConn.Conn().Query(ctx, "select pubname from pg_publication;")
	if err != nil {
		slog.Info("failed to fetch publications", slog.Any("error", err))
		return nil, exceptions.NewFailedPreconditionApiError(fmt.Sprintf("failed to fetch publications: %v", err))
	}

	publications, err := pgx.CollectRows[string](rows, pgx.RowTo)
	if err != nil {
		slog.Info("failed to fetch publications", slog.Any("error", err))
		return nil, exceptions.NewInvalidArgumentApiError(fmt.Sprintf("failed to collect publications: %v", err))
	}
	return &protos.PeerPublicationsResponse{PublicationNames: publications}, nil
}
