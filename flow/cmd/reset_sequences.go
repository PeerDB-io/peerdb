package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"

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

	// Build a safe array literal from table names for inlining into a PL/pgSQL DO block.
	// DO blocks don't accept parameterized arguments, so we escape single quotes manually.
	quotedTables := make([]string, 0, len(destTables))
	for _, t := range destTables {
		quotedTables = append(quotedTables, "'"+strings.ReplaceAll(t, "'", "''")+"'")
	}
	arrayLiteral := "ARRAY[" + strings.Join(quotedTables, ",") + "]::text[]"

	// Single PL/pgSQL block runs entirely server-side: discovers sequences on all tables,
	// resets each to MAX(column), and writes results to a temp table for retrieval.
	doBlock := strings.Replace(`
	DO $$
	DECLARE
	v_table text;
	v_col text;
	v_seq text;
	v_max bigint;
	BEGIN
	CREATE TEMP TABLE IF NOT EXISTS _peerdb_seq_reset_results (
		table_name text,
		column_name text,
		sequence_name text,
		new_value bigint
	) ON COMMIT DROP;

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
		PERFORM setval(v_seq, v_max, v_max > 0);
		INSERT INTO _peerdb_seq_reset_results VALUES (v_table, v_col, v_seq, v_max);
		END LOOP;
	END LOOP;
	END;
	$$`, "$1", arrayLiteral, 1)

	// Run DO block and SELECT in the same transaction so the ON COMMIT DROP temp table
	// is visible to the results query.
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, NewInternalApiError(fmt.Errorf("failed to begin transaction: %w", err))
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err := tx.Exec(ctx, doBlock); err != nil {
		slog.ErrorContext(ctx, "failed to reset sequences", slog.Any("error", err))
		return &protos.ResetMirrorSequencesResponse{
			Ok:           false,
			ErrorMessage: fmt.Sprintf("failed to reset sequences: %v", err),
		}, nil
	}

	rows, err := tx.Query(ctx,
		"SELECT table_name, column_name, sequence_name, new_value FROM _peerdb_seq_reset_results")
	if err != nil {
		slog.ErrorContext(ctx, "failed to read sequence reset results", slog.Any("error", err))
		return &protos.ResetMirrorSequencesResponse{
			Ok:           false,
			ErrorMessage: fmt.Sprintf("failed to read results: %v", err),
		}, nil
	}

	var results []string
	var tableName, colName, seqName string
	var newVal int64
	_, err = pgx.ForEachRow(rows, []any{&tableName, &colName, &seqName, &newVal}, func() error {
		result := fmt.Sprintf("Reset %s to %d (column %s of %s)", seqName, newVal, colName, tableName)
		results = append(results, result)
		slog.InfoContext(ctx, "reset sequence",
			slog.String("sequence", seqName), slog.Int64("value", newVal),
			slog.String("table", tableName), slog.String("column", colName))
		return nil
	})
	if err != nil {
		slog.ErrorContext(ctx, "failed to iterate sequence reset results", slog.Any("error", err))
		return &protos.ResetMirrorSequencesResponse{
			Ok:           false,
			ErrorMessage: fmt.Sprintf("failed to read results: %v", err),
		}, nil
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, NewInternalApiError(fmt.Errorf("failed to commit transaction: %w", err))
	}

	if len(results) == 0 {
		results = append(results, "No serial or identity sequences found on destination tables")
	}

	return &protos.ResetMirrorSequencesResponse{
		Ok:             true,
		ResetSequences: results,
	}, nil
}
