package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
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

func wrapErrorAsFailedPrecondition[T any](value T, err error) (T, APIError) {
	if err != nil {
		return value, NewFailedPreconditionApiError(err)
	}
	return value, nil
}

func (h *FlowRequestHandler) GetPeerInfo(
	ctx context.Context,
	req *protos.PeerInfoRequest,
) (*protos.PeerInfoResponse, APIError) {
	ctx, cancelCtx := context.WithTimeout(ctx, 30*time.Second)
	defer cancelCtx()
	peer, err := connectors.LoadPeer(ctx, h.pool, req.PeerName)
	if err != nil {
		var errNotFound *exceptions.NotFoundError
		if errors.As(err, &errNotFound) {
			return nil, NewNotFoundApiError(err)
		}
		return nil, NewInternalApiError(fmt.Errorf("failed to load peer: %w", err))
	}

	var version string
	versionConn, versionClose, err := connectors.GetAs[connectors.GetVersionConnector](ctx, nil, peer)
	if err != nil {
		if !errors.Is(err, errors.ErrUnsupported) {
			slog.ErrorContext(ctx, "failed to get version connector", slog.Any("error", err))
		}
	} else {
		defer versionClose(ctx)
		version, err = versionConn.GetVersion(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "failed to get version", slog.Any("error", err))
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
) (*protos.PeerTypeResponse, APIError) {
	ctx, cancelCtx := context.WithTimeout(ctx, 30*time.Second)
	defer cancelCtx()
	peer, err := connectors.LoadPeer(ctx, h.pool, req.PeerName)
	if err != nil {
		var errNotFound *exceptions.NotFoundError
		if errors.As(err, &errNotFound) {
			return nil, NewNotFoundApiError(err)
		}
		return nil, NewInternalApiError(fmt.Errorf("failed to load peer: %w", err))
	}

	return &protos.PeerTypeResponse{
		PeerType: peer.Type.String(),
	}, nil
}

func (h *FlowRequestHandler) ListPeers(
	ctx context.Context,
	req *protos.ListPeersRequest,
) (*protos.ListPeersResponse, APIError) {
	query := "SELECT name, type FROM peers"
	if internal.PeerDBOnlyClickHouseAllowed() {
		// only postgres, mysql, mongo,and clickhouse
		query += fmt.Sprintf(" WHERE type IN (%d,%d,%d,%d)",
			protos.DBType_POSTGRES, protos.DBType_MYSQL, protos.DBType_MONGO, protos.DBType_CLICKHOUSE)
	}
	rows, err := h.pool.Query(ctx, query)
	if err != nil {
		slog.ErrorContext(ctx, "failed to query for peers", slog.Any("error", err))
		return nil, NewInternalApiError(fmt.Errorf("failed to query for peers: %w", err))
	}
	peers, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.PeerListItem, error) {
		var peer protos.PeerListItem
		return &peer, row.Scan(&peer.Name, &peer.Type)
	})
	if err != nil {
		slog.ErrorContext(ctx, "failed to collect peers", slog.Any("error", err))
		return nil, NewInternalApiError(fmt.Errorf("failed to collect peers: %w", err))
	}

	sourceItems := make([]*protos.PeerListItem, 0, len(peers))
	destinationItems := make([]*protos.PeerListItem, 0, len(peers))
	for _, peer := range peers {
		if peer.Type == protos.DBType_POSTGRES ||
			peer.Type == protos.DBType_MYSQL ||
			peer.Type == protos.DBType_MONGO ||
			peer.Type == protos.DBType_BIGQUERY {
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
) (*protos.PeerSchemasResponse, APIError) {
	conn, connClose, err := connectors.GetByNameAs[connectors.GetSchemaConnector](ctx, nil, h.pool, req.PeerName)
	if err != nil {
		return nil, NewInvalidArgumentApiError(fmt.Errorf("failed to get schema connector: %w", err))
	}
	defer connClose(ctx)
	return wrapErrorAsFailedPrecondition(conn.GetSchemas(ctx))
}

func (h *FlowRequestHandler) GetTablesInSchema(
	ctx context.Context,
	req *protos.SchemaTablesRequest,
) (*protos.SchemaTablesResponse, APIError) {
	conn, connClose, err := connectors.GetByNameAs[connectors.GetSchemaConnector](ctx, nil, h.pool, req.PeerName)
	if err != nil {
		return nil, NewFailedPreconditionApiError(fmt.Errorf("failed to get schema connector: %w", err))
	}
	defer connClose(ctx)
	return wrapErrorAsFailedPrecondition(conn.GetTablesInSchema(ctx, req.SchemaName, req.CdcEnabled))
}

// Returns list of tables across schema in schema.table format
func (h *FlowRequestHandler) GetAllTables(
	ctx context.Context,
	req *protos.PostgresPeerActivityInfoRequest,
) (*protos.AllTablesResponse, APIError) {
	conn, connClose, err := connectors.GetByNameAs[connectors.GetSchemaConnector](ctx, nil, h.pool, req.PeerName)
	if err != nil {
		return nil, NewFailedPreconditionApiError(fmt.Errorf("failed to get schema connector: %w", err))
	}
	defer connClose(ctx)
	return wrapErrorAsFailedPrecondition(conn.GetAllTables(ctx))
}

func (h *FlowRequestHandler) GetColumns(
	ctx context.Context,
	req *protos.TableColumnsRequest,
) (*protos.TableColumnsResponse, APIError) {
	conn, connClose, err := connectors.GetByNameAs[connectors.GetSchemaConnector](ctx, nil, h.pool, req.PeerName)
	if err != nil {
		return nil, NewFailedPreconditionApiError(fmt.Errorf("failed to get schema connector: %w", err))
	}
	defer connClose(ctx)
	internalVersion, err := internal.PeerDBForceInternalVersion(ctx, nil)
	if err != nil {
		return nil, NewInternalApiError(fmt.Errorf("failed to get internal version: %w", err))
	}
	return wrapErrorAsFailedPrecondition(conn.GetColumns(ctx, internalVersion, req.SchemaName, req.TableName))
}

func (h *FlowRequestHandler) GetColumnsTypeConversion(
	ctx context.Context,
	req *protos.ColumnsTypeConversionRequest,
) (*protos.ColumnsTypeConversionResponse, APIError) {
	return wrapErrorAsFailedPrecondition(connclickhouse.GetColumnsTypeConversion())
}

func (h *FlowRequestHandler) GetSlotInfo(
	ctx context.Context,
	req *protos.PostgresPeerActivityInfoRequest,
) (*protos.PeerSlotResponse, APIError) {
	pgConn, pgClose, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, nil, h.pool, req.PeerName)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to create postgres connector", slog.Any("error", err))
		return nil, NewFailedPreconditionApiError(fmt.Errorf("failed to get postgres connector: %w", err))
	}
	defer pgClose(ctx)

	var customSlotNames []string
	if req.PeerdbManagedOnly {
		customSlotNames, err = h.getCustomSlotNamesForPeer(ctx, req.PeerName)
		if err != nil {
			slog.WarnContext(ctx, "Failed to resolve custom slot names, proceeding without filter", slog.Any("error", err))
			req.PeerdbManagedOnly = false
		}
	}

	slotInfo, err := pgConn.GetSlotInfo(ctx, "", req.PeerdbManagedOnly, customSlotNames)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to get slot info", slog.Any("error", err))
		return nil, NewFailedPreconditionApiError(fmt.Errorf("failed to get slot info: %w", err))
	}

	return &protos.PeerSlotResponse{
		SlotData: slotInfo,
	}, nil
}

func (h *FlowRequestHandler) getCustomSlotNamesForPeer(ctx context.Context, peerName string) ([]string, error) {
	rows, err := h.pool.Query(ctx, `
		SELECT config_proto FROM flows
		WHERE source_peer = (SELECT id FROM peers WHERE name = $1)
			AND query_string IS NULL AND config_proto IS NOT NULL
	`, peerName)
	if err != nil {
		return nil, fmt.Errorf("failed to query flows for peer %s: %w", peerName, err)
	}
	defer rows.Close()

	var customSlotNames []string
	for rows.Next() {
		var configBytes []byte
		if err := rows.Scan(&configBytes); err != nil {
			return nil, fmt.Errorf("failed to scan flow config: %w", err)
		}
		var config protos.FlowConnectionConfigsCore
		if err := proto.Unmarshal(configBytes, &config); err != nil {
			slog.WarnContext(ctx, "Failed to unmarshal flow config", slog.Any("error", err))
			continue
		}
		if config.ReplicationSlotName != "" && !strings.HasPrefix(config.ReplicationSlotName, connpostgres.DefaultSlotPrefix) {
			customSlotNames = append(customSlotNames, config.ReplicationSlotName)
		}
	}
	return customSlotNames, rows.Err()
}

func (h *FlowRequestHandler) GetSlotLagHistory(
	ctx context.Context,
	req *protos.GetSlotLagHistoryRequest,
) (*protos.GetSlotLagHistoryResponse, APIError) {
	rows, err := h.pool.Query(ctx, `select updated_at, slot_size,
			coalesce(redo_lsn,''), coalesce(restart_lsn,''), coalesce(confirmed_flush_lsn,'')
		from peerdb_stats.peer_slot_size
		where slot_size is not null
			and peer_name = $1
			and slot_name = $2
			and updated_at > (now()-$3::INTERVAL)
		order by random() limit 720`, req.PeerName, req.SlotName, req.TimeSince)
	if err != nil {
		return nil, NewInternalApiError(fmt.Errorf("failed to get slot lag history: %w", err))
	}
	points, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.SlotLagPoint, error) {
		var updatedAt time.Time
		var slotSize int64
		var redoLSN string
		var restartLSN string
		var confirmedFlushLSN string
		if err := row.Scan(&updatedAt, &slotSize, &redoLSN, &restartLSN, &confirmedFlushLSN); err != nil {
			return nil, NewInternalApiError(fmt.Errorf("failed to scan slot lag history: %w", err))
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
		return nil, NewInternalApiError(fmt.Errorf("failed to collect slot lag history: %w", err))
	}

	return &protos.GetSlotLagHistoryResponse{Data: points}, nil
}

func (h *FlowRequestHandler) GetStatInfo(
	ctx context.Context,
	req *protos.PostgresPeerActivityInfoRequest,
) (*protos.PeerStatResponse, APIError) {
	peerConn, peerClose, err := connectors.GetByNameAs[connectors.StatActivityConnector](ctx, nil, h.pool, req.PeerName)
	if err != nil {
		return nil, NewFailedPreconditionApiError(fmt.Errorf("failed to get stat activity connector: %w", err))
	}
	defer peerClose(ctx)

	return wrapErrorAsFailedPrecondition(peerConn.StatActivity(ctx, req))
}

func (h *FlowRequestHandler) GetPublications(
	ctx context.Context,
	req *protos.PostgresPeerActivityInfoRequest,
) (*protos.PeerPublicationsResponse, APIError) {
	peerConn, peerClose, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, nil, h.pool, req.PeerName)
	if err != nil {
		return nil, NewFailedPreconditionApiError(fmt.Errorf("failed to get postgres connector: %w", err))
	}
	defer peerClose(ctx)

	rows, err := peerConn.Conn().Query(ctx, "select pubname from pg_publication;")
	if err != nil {
		slog.InfoContext(ctx, "failed to fetch publications", slog.Any("error", err))
		return nil, NewFailedPreconditionApiError(fmt.Errorf("failed to fetch publications: %w", err))
	}

	publications, err := pgx.CollectRows[string](rows, pgx.RowTo)
	if err != nil {
		slog.InfoContext(ctx, "failed to fetch publications", slog.Any("error", err))
		return nil, NewInvalidArgumentApiError(fmt.Errorf("failed to collect publications: %w", err))
	}
	return &protos.PeerPublicationsResponse{PublicationNames: publications}, nil
}
