package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/proto"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/generated/protos"
)

func (h *FlowRequestHandler) getPGPeerConfig(ctx context.Context, peerName string) (*protos.PostgresConfig, error) {
	var pgPeerOptions sql.RawBytes
	var pgPeerConfig protos.PostgresConfig
	err := h.pool.QueryRow(ctx,
		"SELECT options FROM peers WHERE name = $1 AND type=3", peerName).Scan(&pgPeerOptions)
	if err != nil {
		return nil, err
	}

	unmarshalErr := proto.Unmarshal(pgPeerOptions, &pgPeerConfig)
	if err != nil {
		return nil, unmarshalErr
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

func (h *FlowRequestHandler) GetSchemas(
	ctx context.Context,
	req *protos.PostgresPeerActivityInfoRequest,
) (*protos.PeerSchemasResponse, error) {
	tunnel, peerConn, err := h.getConnForPGPeer(ctx, req.PeerName)
	if err != nil {
		return &protos.PeerSchemasResponse{Schemas: nil}, err
	}
	defer tunnel.Close()
	defer peerConn.Close(ctx)

	rows, err := peerConn.Query(ctx, "SELECT nspname"+
		" FROM pg_namespace WHERE nspname !~ '^pg_' AND nspname <> 'information_schema';")
	if err != nil {
		return &protos.PeerSchemasResponse{Schemas: nil}, err
	}

	schemas, err := pgx.CollectRows[string](rows, pgx.RowTo)
	if err != nil {
		return &protos.PeerSchemasResponse{Schemas: nil}, err
	}
	return &protos.PeerSchemasResponse{Schemas: schemas}, nil
}

func (h *FlowRequestHandler) GetTablesInSchema(
	ctx context.Context,
	req *protos.SchemaTablesRequest,
) (*protos.SchemaTablesResponse, error) {
	tunnel, peerConn, err := h.getConnForPGPeer(ctx, req.PeerName)
	if err != nil {
		return &protos.SchemaTablesResponse{Tables: nil}, err
	}
	defer tunnel.Close()
	defer peerConn.Close(ctx)

	rows, err := peerConn.Query(ctx, `SELECT DISTINCT ON (t.relname)
    t.relname,
    CASE
        WHEN con.contype = 'p' OR t.relreplident = 'i' OR t.relreplident = 'f' THEN true
        ELSE false
    END AS can_mirror
	FROM
		pg_class t
	LEFT JOIN
		pg_namespace n ON t.relnamespace = n.oid
	LEFT JOIN
		pg_constraint con ON con.conrelid = t.oid AND con.contype = 'p'
	WHERE
		n.nspname = $1
	AND
		t.relkind = 'r'
	ORDER BY
    t.relname,
    can_mirror DESC;
`, req.SchemaName)
	if err != nil {
		return &protos.SchemaTablesResponse{Tables: nil}, err
	}

	defer rows.Close()
	var tables []*protos.TableResponse
	for rows.Next() {
		var table pgtype.Text
		var hasPkeyOrReplica pgtype.Bool
		err := rows.Scan(&table, &hasPkeyOrReplica)
		if err != nil {
			return &protos.SchemaTablesResponse{Tables: nil}, err
		}
		canMirror := false
		if hasPkeyOrReplica.Valid && hasPkeyOrReplica.Bool {
			canMirror = true
		}

		tables = append(tables, &protos.TableResponse{
			TableName: table.String,
			CanMirror: canMirror,
		})
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
		"WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND c.relkind = 'r';")
	if err != nil {
		return &protos.AllTablesResponse{Tables: nil}, err
	}

	defer rows.Close()
	var tables []string
	for rows.Next() {
		var table pgtype.Text
		err := rows.Scan(&table)
		if err != nil {
			return &protos.AllTablesResponse{Tables: nil}, err
		}

		tables = append(tables, table.String)
	}
	return &protos.AllTablesResponse{Tables: tables}, nil
}

func (h *FlowRequestHandler) GetColumns(
	ctx context.Context,
	req *protos.TableColumnsRequest,
) (*protos.TableColumnsResponse, error) {
	tunnel, peerConn, err := h.getConnForPGPeer(ctx, req.PeerName)
	if err != nil {
		return &protos.TableColumnsResponse{Columns: nil}, err
	}
	defer tunnel.Close()
	defer peerConn.Close(ctx)

	rows, err := peerConn.Query(ctx, `
	SELECT
    attname AS column_name,
    format_type(atttypid, atttypmod) AS data_type,
    CASE
        WHEN attnum = ANY(conkey) THEN true
        ELSE false
    END AS is_primary_key
	FROM
		pg_attribute
	JOIN
		pg_class ON pg_attribute.attrelid = pg_class.oid
	LEFT JOIN
		pg_constraint ON pg_attribute.attrelid = pg_constraint.conrelid
		AND pg_attribute.attnum = ANY(pg_constraint.conkey)
	WHERE
		relnamespace::regnamespace::text = $1
		AND
		relname = $2
		AND pg_attribute.attnum > 0
		AND NOT attisdropped
	ORDER BY
    attnum;
	`, req.SchemaName, req.TableName)
	if err != nil {
		return &protos.TableColumnsResponse{Columns: nil}, err
	}

	defer rows.Close()
	var columns []string
	for rows.Next() {
		var columnName pgtype.Text
		var datatype pgtype.Text
		var isPkey pgtype.Bool
		err := rows.Scan(&columnName, &datatype, &isPkey)
		if err != nil {
			return &protos.TableColumnsResponse{Columns: nil}, err
		}
		column := fmt.Sprintf("%s:%s:%v", columnName.String, datatype.String, isPkey.Bool)
		columns = append(columns, column)
	}
	return &protos.TableColumnsResponse{Columns: columns}, nil
}

func (h *FlowRequestHandler) GetSlotInfo(
	ctx context.Context,
	req *protos.PostgresPeerActivityInfoRequest,
) (*protos.PeerSlotResponse, error) {
	pgConfig, err := h.getPGPeerConfig(ctx, req.PeerName)
	if err != nil {
		return &protos.PeerSlotResponse{SlotData: nil}, err
	}

	pgConnector, err := connpostgres.NewPostgresConnector(ctx, pgConfig)
	if err != nil {
		slog.Error("Failed to create postgres connector", slog.Any("error", err))
		return &protos.PeerSlotResponse{SlotData: nil}, err
	}
	defer pgConnector.Close()

	slotInfo, err := pgConnector.GetSlotInfo(ctx, "")
	if err != nil {
		slog.Error("Failed to get slot info", slog.Any("error", err))
		return &protos.PeerSlotResponse{SlotData: nil}, err
	}

	return &protos.PeerSlotResponse{
		SlotData: slotInfo,
	}, nil
}

func (h *FlowRequestHandler) GetStatInfo(
	ctx context.Context,
	req *protos.PostgresPeerActivityInfoRequest,
) (*protos.PeerStatResponse, error) {
	tunnel, peerConn, err := h.getConnForPGPeer(ctx, req.PeerName)
	if err != nil {
		return &protos.PeerStatResponse{StatData: nil}, err
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
		return &protos.PeerStatResponse{StatData: nil}, err
	}
	defer rows.Close()
	var statInfoRows []*protos.StatInfo
	for rows.Next() {
		var pid int64
		var waitEvent sql.NullString
		var waitEventType sql.NullString
		var queryStart sql.NullString
		var query sql.NullString
		var duration sql.NullFloat64

		err := rows.Scan(&pid, &waitEvent, &waitEventType, &queryStart, &query, &duration)
		if err != nil {
			slog.Error("Failed to scan row", slog.Any("error", err))
			return &protos.PeerStatResponse{StatData: nil}, err
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

		statInfoRows = append(statInfoRows, &protos.StatInfo{
			Pid:           pid,
			WaitEvent:     we,
			WaitEventType: wet,
			QueryStart:    qs,
			Query:         q,
			Duration:      float32(d),
		})
	}

	return &protos.PeerStatResponse{
		StatData: statInfoRows,
	}, nil
}
