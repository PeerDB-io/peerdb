package cmd

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
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
		return nil, err
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
		return nil, err
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
		// only postgres, mysql, and clickhouse
		query += " WHERE type IN (3, 7, 8)"
	}
	rows, err := h.pool.Query(ctx, query)
	if err != nil {
		slog.Error("failed to query for peers", slog.Any("error", err))
		return nil, err
	}
	peers, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.PeerListItem, error) {
		var peer protos.PeerListItem
		return &peer, row.Scan(&peer.Name, &peer.Type)
	})
	if err != nil {
		slog.Error("failed to collect peers", slog.Any("error", err))
		return nil, err
	}

	sourceItems := make([]*protos.PeerListItem, 0, len(peers))
	destinationItems := make([]*protos.PeerListItem, 0, len(peers))
	for _, peer := range peers {
		if peer.Type == protos.DBType_POSTGRES || peer.Type == protos.DBType_MYSQL {
			sourceItems = append(sourceItems, peer)
		}
		if peer.Type != protos.DBType_MYSQL && (!internal.PeerDBOnlyClickHouseAllowed() || peer.Type == protos.DBType_CLICKHOUSE) {
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
		return nil, err
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
		return nil, err
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
		return nil, err
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
		return nil, err
	}
	defer connectors.CloseConnector(ctx, conn)
	return conn.GetColumns(ctx, req.SchemaName, req.TableName)
}

func (h *FlowRequestHandler) GetSlotInfo(
	ctx context.Context,
	req *protos.PostgresPeerActivityInfoRequest,
) (*protos.PeerSlotResponse, error) {
	pgConnector, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, nil, h.pool, req.PeerName)
	if err != nil {
		slog.Error("Failed to create postgres connector", slog.Any("error", err))
		return nil, err
	}
	defer connectors.CloseConnector(ctx, pgConnector)

	slotInfo, err := pgConnector.GetSlotInfo(ctx, "")
	if err != nil {
		slog.Error("Failed to get slot info", slog.Any("error", err))
		return nil, err
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
		return nil, err
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
		return nil, err
	}

	return &protos.GetSlotLagHistoryResponse{Data: points}, nil
}

func (h *FlowRequestHandler) GetStatInfo(
	ctx context.Context,
	req *protos.PostgresPeerActivityInfoRequest,
) (*protos.PeerStatResponse, error) {
	peerConn, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, nil, h.pool, req.PeerName)
	if err != nil {
		return nil, err
	}
	defer connectors.CloseConnector(ctx, peerConn)

	peerUser := peerConn.Config.User

	rows, err := peerConn.Conn().Query(ctx, "SELECT pid, wait_event, wait_event_type, query_start::text, query,"+
		"EXTRACT(epoch FROM(now()-query_start)) AS dur, state"+
		" FROM pg_stat_activity WHERE "+
		"usename=$1 AND application_name LIKE 'peerdb%';", peerUser)
	if err != nil {
		slog.Error("Failed to get stat info", slog.Any("error", err))
		return nil, err
	}

	statInfoRows, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.StatInfo, error) {
		var pid int64
		var waitEvent sql.NullString
		var waitEventType sql.NullString
		var queryStart sql.NullString
		var query sql.NullString
		var duration sql.NullFloat64
		// shouldn't be null
		var state string

		err := rows.Scan(&pid, &waitEvent, &waitEventType, &queryStart, &query, &duration, &state)
		if err != nil {
			slog.Error("Failed to scan row", slog.Any("error", err))
			return nil, err
		}

		we := waitEvent.String
		if !waitEvent.Valid {
			we = ""
		}

		wet := waitEventType.String
		if !waitEventType.Valid {
			wet = ""
		}

		q := query.String
		if !query.Valid {
			q = ""
		}

		qs := queryStart.String
		if !queryStart.Valid {
			qs = ""
		}

		d := duration.Float64
		if !duration.Valid {
			d = -1
		}

		return &protos.StatInfo{
			Pid:           pid,
			WaitEvent:     we,
			WaitEventType: wet,
			QueryStart:    qs,
			Query:         q,
			Duration:      float32(d),
			State:         state,
		}, nil
	})
	if err != nil {
		return nil, err
	}

	return &protos.PeerStatResponse{
		StatData: statInfoRows,
	}, nil
}

func (h *FlowRequestHandler) GetPublications(
	ctx context.Context,
	req *protos.PostgresPeerActivityInfoRequest,
) (*protos.PeerPublicationsResponse, error) {
	peerConn, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, nil, h.pool, req.PeerName)
	if err != nil {
		return nil, err
	}
	defer connectors.CloseConnector(ctx, peerConn)

	rows, err := peerConn.Conn().Query(ctx, "select pubname from pg_publication;")
	if err != nil {
		slog.Info("failed to fetch publications", slog.Any("error", err))
		return nil, err
	}

	publications, err := pgx.CollectRows[string](rows, pgx.RowTo)
	if err != nil {
		slog.Info("failed to fetch publications", slog.Any("error", err))
		return nil, err
	}
	return &protos.PeerPublicationsResponse{PublicationNames: publications}, nil
}
