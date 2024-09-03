package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/PeerDB-io/peer-flow/connectors"
	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
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

func (h *FlowRequestHandler) getPGPeerConfig(ctx context.Context, peerName string) (*protos.PostgresConfig, error) {
	var encPeerOptions []byte
	var encKeyID string
	err := h.pool.QueryRow(ctx,
		"SELECT options, enc_key_id FROM peers WHERE name=$1 AND type=3", peerName).Scan(&encPeerOptions, &encKeyID)
	if err != nil {
		return nil, err
	}

	peerOptions, err := peerdbenv.Decrypt(encKeyID, encPeerOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to load peer: %w", err)
	}

	var pgPeerConfig protos.PostgresConfig
	if err := proto.Unmarshal(peerOptions, &pgPeerConfig); err != nil {
		return nil, err
	}

	return &pgPeerConfig, nil
}

func (h *FlowRequestHandler) getConnForPGPeer(ctx context.Context, peerName string) (*connpostgres.SSHTunnel, *pgx.Conn, error) {
	pgPeerConfig, err := h.getPGPeerConfig(ctx, peerName)
	if err != nil {
		return nil, nil, err
	}

	tunnel, err := connpostgres.NewSSHTunnel(ctx, pgPeerConfig.SshConfig)
	if err != nil {
		slog.Error("Failed to create postgres pool", slog.Any("error", err))
		return nil, nil, err
	}

	conn, err := tunnel.NewPostgresConnFromPostgresConfig(ctx, pgPeerConfig)
	if err != nil {
		tunnel.Close()
		return nil, nil, err
	}

	return tunnel, conn, nil
}

func (h *FlowRequestHandler) GetPeerInfo(
	ctx context.Context,
	req *protos.PeerInfoRequest,
) (*protos.Peer, error) {
	peer, err := connectors.LoadPeer(ctx, h.pool, req.PeerName)
	if err != nil {
		return nil, err
	}

	peer.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Kind() == protoreflect.MessageKind {
			redactProto(v.Message().Interface())
		}
		return true
	})
	return peer, nil
}

func (h *FlowRequestHandler) ListPeers(
	ctx context.Context,
	req *protos.ListPeersRequest,
) (*protos.ListPeersResponse, error) {
	query := "SELECT name, type FROM peers"
	if peerdbenv.PeerDBAllowedTargets() == strings.ToLower(protos.DBType_CLICKHOUSE.String()) {
		// only postgres and clickhouse peers
		query += " WHERE type IN (3, 8)"
	}
	rows, err := h.pool.Query(ctx, query)
	if err != nil {
		slog.Error("failed to query for peers", slog.Any("error", err))
		return nil, err
	}
	peers, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.PeerListItem, error) {
		var peer protos.PeerListItem
		err := row.Scan(&peer.Name, &peer.Type)
		return &peer, err
	})
	if err != nil {
		slog.Error("failed to collect peers", slog.Any("error", err))
		return nil, err
	}

	sourceItems := make([]*protos.PeerListItem, 0, len(peers))
	for _, peer := range peers {
		// only postgres peers
		if peer.Type == 3 {
			sourceItems = append(sourceItems, peer)
		}
	}

	destinationItems := peers
	if peerdbenv.PeerDBAllowedTargets() == strings.ToLower(protos.DBType_CLICKHOUSE.String()) {
		destinationItems = make([]*protos.PeerListItem, 0, len(peers))
		for _, peer := range peers {
			// only clickhouse peers
			if peer.Type == 8 {
				destinationItems = append(destinationItems, peer)
			}
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
	tunnel, peerConn, err := h.getConnForPGPeer(ctx, req.PeerName)
	if err != nil {
		return nil, err
	}
	defer tunnel.Close()
	defer peerConn.Close(ctx)

	rows, err := peerConn.Query(ctx, "SELECT nspname"+
		" FROM pg_namespace WHERE nspname !~ '^pg_' AND nspname <> 'information_schema';")
	if err != nil {
		return nil, err
	}

	schemas, err := pgx.CollectRows[string](rows, pgx.RowTo)
	if err != nil {
		return nil, err
	}
	return &protos.PeerSchemasResponse{Schemas: schemas}, nil
}

func (h *FlowRequestHandler) GetTablesInSchema(
	ctx context.Context,
	req *protos.SchemaTablesRequest,
) (*protos.SchemaTablesResponse, error) {
	tunnel, peerConn, err := h.getConnForPGPeer(ctx, req.PeerName)
	if err != nil {
		return nil, err
	}
	defer tunnel.Close()
	defer peerConn.Close(ctx)

	pgVersion, err := shared.GetMajorVersion(ctx, peerConn)
	if err != nil {
		slog.Error("unable to get pgversion for schema tables", slog.Any("error", err))
		return nil, err
	}

	relKindFilterExpr := "t.relkind IN ('r', 'p')"
	// publish_via_partition_root is only available in PG13 and above
	if pgVersion < shared.POSTGRES_13 {
		relKindFilterExpr = "t.relkind = 'r'"
	}

	rows, err := peerConn.Query(ctx, `SELECT DISTINCT ON (t.relname)
		t.relname,
		(con.contype = 'p' OR t.relreplident in ('i', 'f')) AS can_mirror,
		pg_size_pretty(pg_total_relation_size(t.oid))::text AS table_size
	FROM pg_class t
	LEFT JOIN pg_namespace n ON t.relnamespace = n.oid
	LEFT JOIN pg_constraint con ON con.conrelid = t.oid
	WHERE n.nspname = $1 AND `+
		relKindFilterExpr+
		`AND t.relispartition IS NOT TRUE ORDER BY t.relname, can_mirror DESC;
`, req.SchemaName)
	if err != nil {
		slog.Info("failed to fetch tables", slog.Any("error", err))
		return nil, err
	}

	tables, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.TableResponse, error) {
		var table pgtype.Text
		var hasPkeyOrReplica pgtype.Bool
		var tableSize pgtype.Text
		if err := rows.Scan(&table, &hasPkeyOrReplica, &tableSize); err != nil {
			return nil, err
		}
		var sizeOfTable string
		if tableSize.Valid {
			sizeOfTable = tableSize.String
		}
		canMirror := false
		if !req.CdcEnabled || (hasPkeyOrReplica.Valid && hasPkeyOrReplica.Bool) {
			canMirror = true
		}

		return &protos.TableResponse{
			TableName: table.String,
			CanMirror: canMirror,
			TableSize: sizeOfTable,
		}, nil
	})
	if err != nil {
		slog.Info("failed to fetch publications", slog.Any("error", err))
		return nil, err
	}
	return &protos.SchemaTablesResponse{Tables: tables}, nil
}

// Returns list of tables across schema in schema.table format
func (h *FlowRequestHandler) GetAllTables(
	ctx context.Context,
	req *protos.PostgresPeerActivityInfoRequest,
) (*protos.AllTablesResponse, error) {
	tunnel, peerConn, err := h.getConnForPGPeer(ctx, req.PeerName)
	if err != nil {
		return &protos.AllTablesResponse{Tables: nil}, err
	}
	defer tunnel.Close()
	defer peerConn.Close(ctx)

	rows, err := peerConn.Query(ctx, "SELECT n.nspname || '.' || c.relname AS schema_table "+
		"FROM pg_class c "+
		"JOIN pg_namespace n ON c.relnamespace = n.oid "+
		"WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'"+
		" AND c.relkind IN ('r', 'v', 'm', 'f', 'p')")
	if err != nil {
		return nil, err
	}

	tables, err := pgx.CollectRows[string](rows, pgx.RowTo)
	if err != nil {
		return nil, err
	}
	return &protos.AllTablesResponse{Tables: tables}, nil
}

func (h *FlowRequestHandler) GetColumns(
	ctx context.Context,
	req *protos.TableColumnsRequest,
) (*protos.TableColumnsResponse, error) {
	tunnel, peerConn, err := h.getConnForPGPeer(ctx, req.PeerName)
	if err != nil {
		return nil, err
	}
	defer tunnel.Close()
	defer peerConn.Close(ctx)

	rows, err := peerConn.Query(ctx, `SELECT
		distinct attname AS column_name,
		format_type(atttypid, atttypmod) AS data_type,
		(attnum = ANY(conkey)) AS is_primary_key
	FROM pg_attribute
	JOIN pg_class ON pg_attribute.attrelid = pg_class.oid
	JOIN pg_namespace on pg_class.relnamespace = pg_namespace.oid
	LEFT JOIN pg_constraint ON pg_attribute.attrelid = pg_constraint.conrelid
		AND pg_attribute.attnum = ANY(pg_constraint.conkey)
	WHERE pg_namespace.nspname = $1
		AND relname = $2
		AND pg_attribute.attnum > 0
		AND NOT attisdropped
	ORDER BY column_name`, req.SchemaName, req.TableName)
	if err != nil {
		return nil, err
	}

	columns, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (string, error) {
		var columnName pgtype.Text
		var datatype pgtype.Text
		var isPkey pgtype.Bool
		err := rows.Scan(&columnName, &datatype, &isPkey)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s:%s:%v", columnName.String, datatype.String, isPkey.Bool), nil
	})
	if err != nil {
		return nil, err
	}
	return &protos.TableColumnsResponse{Columns: columns}, nil
}

func (h *FlowRequestHandler) GetSlotInfo(
	ctx context.Context,
	req *protos.PostgresPeerActivityInfoRequest,
) (*protos.PeerSlotResponse, error) {
	pgConfig, err := h.getPGPeerConfig(ctx, req.PeerName)
	if err != nil {
		return nil, err
	}

	pgConnector, err := connpostgres.NewPostgresConnector(ctx, pgConfig)
	if err != nil {
		slog.Error("Failed to create postgres connector", slog.Any("error", err))
		return nil, err
	}
	defer pgConnector.Close()

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
	rows, err := h.pool.Query(ctx, `select updated_at, slot_size
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
		if err := row.Scan(&updatedAt, &slotSize); err != nil {
			return nil, err
		}
		return &protos.SlotLagPoint{
			UpdatedAt: float64(updatedAt.UnixMilli()),
			SlotSize:  float64(slotSize) / 1000.0,
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
	tunnel, peerConn, err := h.getConnForPGPeer(ctx, req.PeerName)
	if err != nil {
		return nil, err
	}
	defer tunnel.Close()
	defer peerConn.Close(ctx)

	peerUser := peerConn.Config().User

	rows, err := peerConn.Query(ctx, "SELECT pid, wait_event, wait_event_type, query_start::text, query,"+
		"EXTRACT(epoch FROM(now()-query_start)) AS dur"+
		" FROM pg_stat_activity WHERE "+
		"usename=$1 AND state != 'idle';", peerUser)
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

		err := rows.Scan(&pid, &waitEvent, &waitEventType, &queryStart, &query, &duration)
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
	tunnel, peerConn, err := h.getConnForPGPeer(ctx, req.PeerName)
	if err != nil {
		return nil, err
	}
	defer tunnel.Close()
	defer peerConn.Close(ctx)

	rows, err := peerConn.Query(ctx, "select pubname from pg_publication;")
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
