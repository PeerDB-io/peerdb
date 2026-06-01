package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func (h *FlowRequestHandler) ResetMirrorSequences(
	ctx context.Context,
	req *protos.ResetMirrorSequencesRequest,
) (*protos.ResetMirrorSequencesResponse, APIError) {
	config, err := h.getFlowConfigFromCatalog(ctx, req.FlowJobName)
	if err != nil {
		return nil, NewInternalApiError(fmt.Errorf("failed to get flow config: %w", err))
	}

	dstType, err := connectors.LoadPeerType(ctx, h.pool, config.DestinationName)
	if err != nil {
		return nil, NewInternalApiError(fmt.Errorf("failed to load destination peer type: %w", err))
	}

	if dstType != protos.DBType_POSTGRES {
		return nil, NewFailedPreconditionApiError(
			fmt.Errorf("reset sequences is only supported for PostgreSQL destinations"))
	}

	if config.System != protos.TypeSystem_PG {
		return nil, NewFailedPreconditionApiError(
			fmt.Errorf("reset sequences is only supported for mirrors using the PG type system"))
	}

	dstConn, dstClose, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, nil, h.pool, config.DestinationName)
	if err != nil {
		return nil, NewInternalApiError(fmt.Errorf("failed to get destination postgres connector: %w", err))
	}
	defer dstClose(ctx)

	conn := dstConn.Conn()

	destTables := make([]string, 0, len(config.TableMappings))
	for _, tm := range config.TableMappings {
		destTables = append(destTables, tm.DestinationTableIdentifier)
	}

	quotedTables := make([]string, 0, len(destTables))
	for _, t := range destTables {
		quotedTables = append(quotedTables, "'"+strings.ReplaceAll(t, "'", "''")+"'")
	}
	arrayLiteral := "ARRAY[" + strings.Join(quotedTables, ",") + "]::text[]"

	// Discover sequences on all tables all server-side
	// resets each to MAX(column).
	doBlock := strings.Replace(`
	DO $$
	DECLARE
	v_table text;
	v_col text;
	v_seq text;
	v_max bigint;
	BEGIN
	FOREACH v_table IN ARRAY $1
	LOOP
		FOR v_col, v_seq IN
		SELECT a.attname, pg_get_serial_sequence(v_table, a.attname)
		FROM pg_attribute a
		WHERE a.attrelid = v_table::regclass
			AND a.attnum > 0
			AND NOT a.attisdropped
			AND pg_get_serial_sequence(v_table, a.attname) IS NOT NULL
		LOOP
		EXECUTE format('SELECT COALESCE(MAX(%I), 0) FROM %s', v_col, v_table) INTO v_max;
		IF v_max > 0 THEN
			PERFORM setval(v_seq, v_max, true);
		END IF;
		END LOOP;
	END LOOP;
	END;
	$$`, "$1", arrayLiteral, 1)

	if _, err := conn.Exec(ctx, doBlock); err != nil {
		slog.ErrorContext(ctx, "failed to reset sequences", slog.Any("error", err))
		return &protos.ResetMirrorSequencesResponse{
			Ok:           false,
			ErrorMessage: fmt.Sprintf("failed to reset sequences: %v", err),
		}, nil
	}

	return &protos.ResetMirrorSequencesResponse{
		Ok: true,
	}, nil
}
