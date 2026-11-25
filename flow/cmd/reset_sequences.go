package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
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
		parsed, err := common.ParseTableIdentifier(tm.DestinationTableIdentifier)
		if err != nil {
			return nil, NewInternalApiError(fmt.Errorf("failed to parse destination table %s: %w",
				tm.DestinationTableIdentifier, err))
		}
		destTables = append(destTables, parsed.String())
	}

	quotedTables := make([]string, 0, len(destTables))
	for _, t := range destTables {
		quotedTables = append(quotedTables, "'"+strings.ReplaceAll(t, "'", "''")+"'")
	}
	arrayLiteral := "ARRAY[" + strings.Join(quotedTables, ",") + "]::text[]"

	// Reset each sequence to the greatest value across its mapped columns.
	doBlock := strings.Replace(`
	DO $$
	DECLARE
	v_table text;
	v_col text;
	v_seq regclass;
	v_max bigint;
	v_seqs regclass[] := ARRAY[]::regclass[];
	v_maxes bigint[] := ARRAY[]::bigint[];
	v_index integer;
	BEGIN
	FOREACH v_table IN ARRAY $1
	LOOP
		FOR v_col, v_seq IN
		SELECT a.attname, sequences.seq
		FROM pg_attribute a
		CROSS JOIN LATERAL (
			SELECT pg_get_serial_sequence(v_table, a.attname)::regclass AS seq
			UNION
			SELECT d.refobjid::regclass
			FROM pg_attrdef ad
			JOIN pg_depend d ON d.classid = 'pg_attrdef'::regclass
				AND d.objid = ad.oid AND d.refclassid = 'pg_class'::regclass
			JOIN pg_class s ON s.oid = d.refobjid AND s.relkind = 'S'
			WHERE ad.adrelid = a.attrelid AND ad.adnum = a.attnum
		) sequences
		WHERE a.attrelid = v_table::regclass
			AND a.attnum > 0
			AND NOT a.attisdropped
			AND sequences.seq IS NOT NULL
		LOOP
		EXECUTE format('SELECT COALESCE(MAX(%I), 0) FROM %s', v_col, v_table) INTO v_max;
		IF v_max > 0 THEN
			v_index := array_position(v_seqs, v_seq);
			IF v_index IS NULL THEN
				v_seqs := array_append(v_seqs, v_seq);
				v_maxes := array_append(v_maxes, v_max);
			ELSE
				v_maxes[v_index] := GREATEST(v_maxes[v_index], v_max);
			END IF;
		END IF;
		END LOOP;
	END LOOP;
	FOR v_index IN 1..COALESCE(array_length(v_seqs, 1), 0)
	LOOP
		PERFORM setval(v_seqs[v_index], v_maxes[v_index], true);
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
