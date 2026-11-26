package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

type sequenceRef struct {
	Schema string
	Name   string
}

func (s sequenceRef) String() string {
	return common.QuoteIdentifier(s.Schema) + "." + common.QuoteIdentifier(s.Name)
}

type columnRef struct {
	Table  common.QualifiedTable
	Column string
}

// Every sequence a column draws values from: owned (serial/identity), referenced by
// DEFAULT nextval('seq'::regclass), or referenced by a legacy DEFAULT nextval('seq'::text),
// which leaves no pg_depend entry and so has to be parsed out of the default expression.
const columnSequencesQuery = `
SELECT n.nspname, c.relname, a.attname, sn.nspname, s.relname
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL (
	SELECT d.objid AS seqoid
	FROM pg_depend d
	WHERE d.classid = 'pg_class'::regclass AND d.refclassid = 'pg_class'::regclass
		AND d.refobjid = a.attrelid AND d.refobjsubid = a.attnum AND d.deptype IN ('a', 'i')
	UNION
	SELECT d.refobjid
	FROM pg_attrdef ad
	JOIN pg_depend d ON d.classid = 'pg_attrdef'::regclass AND d.objid = ad.oid
		AND d.refclassid = 'pg_class'::regclass
	WHERE ad.adrelid = a.attrelid AND ad.adnum = a.attnum
	UNION
	SELECT to_regclass(substring(pg_get_expr(ad.adbin, ad.adrelid) FROM 'nextval\(+''([^'']+)'''))::oid
	FROM pg_attrdef ad
	WHERE ad.adrelid = a.attrelid AND ad.adnum = a.attnum
) seqs
JOIN pg_class s ON s.oid = seqs.seqoid AND s.relkind = 'S'
JOIN pg_namespace sn ON sn.oid = s.relnamespace
WHERE a.attrelid = ANY($1::text[]::regclass[]) AND a.attnum > 0 AND NOT a.attisdropped`

func queryColumnSequences(ctx context.Context, conn *pgx.Conn, tables []string) (map[columnRef][]sequenceRef, error) {
	rows, err := conn.Query(ctx, columnSequencesQuery, tables)
	if err != nil {
		return nil, err
	}
	result := make(map[columnRef][]sequenceRef)
	var tableSchema, tableName, column, seqSchema, seqName string
	_, err = pgx.ForEachRow(rows, []any{&tableSchema, &tableName, &column, &seqSchema, &seqName}, func() error {
		col := columnRef{common.QualifiedTable{Namespace: tableSchema, Table: tableName}, column}
		result[col] = append(result[col], sequenceRef{seqSchema, seqName})
		return nil
	})
	return result, err
}

type sourceSequence struct {
	ref       sequenceRef
	lastValue *int64
}

func querySourceSequences(ctx context.Context, conn *pgx.Conn, schemas []string) ([]sourceSequence, error) {
	rows, err := conn.Query(ctx,
		"SELECT schemaname, sequencename, last_value FROM pg_sequences WHERE schemaname = ANY($1::text[]) ORDER BY 1, 2",
		schemas)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (sourceSequence, error) {
		var seq sourceSequence
		err := row.Scan(&seq.ref.Schema, &seq.ref.Name, &seq.lastValue)
		return seq, err
	})
}

func queryExistingSequences(ctx context.Context, conn *pgx.Conn, schemas []string) (map[sequenceRef]struct{}, error) {
	rows, err := conn.Query(ctx,
		"SELECT schemaname, sequencename FROM pg_sequences WHERE schemaname = ANY($1::text[])", schemas)
	if err != nil {
		return nil, err
	}
	result := make(map[sequenceRef]struct{})
	var ref sequenceRef
	_, err = pgx.ForEachRow(rows, []any{&ref.Schema, &ref.Name}, func() error {
		result[ref] = struct{}{}
		return nil
	})
	return result, err
}

// ResetMirrorSequences brings destination sequences in line with the source after a
// PG->PG migration. Every used sequence in the source schemas is copied to the destination
// sequence of the same name; sequences backing mapped columns are additionally mapped
// through the table mapping so renamed tables are covered. Destination sequences backing a
// mapped column are never set below MAX(column), so inserts cannot collide with synced rows.
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

	srcType, err := connectors.LoadPeerType(ctx, h.pool, config.SourceName)
	if err != nil {
		return nil, NewInternalApiError(fmt.Errorf("failed to load source peer type: %w", err))
	}
	if srcType != protos.DBType_POSTGRES {
		return nil, NewFailedPreconditionApiError(
			fmt.Errorf("reset sequences is only supported for PostgreSQL sources"))
	}

	if config.System != protos.TypeSystem_PG {
		return nil, NewFailedPreconditionApiError(
			fmt.Errorf("reset sequences is only supported for mirrors using the PG type system"))
	}

	srcTables := make([]string, 0, len(config.TableMappings))
	dstTables := make([]string, 0, len(config.TableMappings))
	tableMap := make(map[common.QualifiedTable]common.QualifiedTable, len(config.TableMappings))
	var srcSchemas []string
	for _, tm := range config.TableMappings {
		src, err := common.ParseTableIdentifier(tm.SourceTableIdentifier)
		if err != nil {
			return nil, NewInternalApiError(fmt.Errorf("failed to parse source table %s: %w",
				tm.SourceTableIdentifier, err))
		}
		dst, err := common.ParseTableIdentifier(tm.DestinationTableIdentifier)
		if err != nil {
			return nil, NewInternalApiError(fmt.Errorf("failed to parse destination table %s: %w",
				tm.DestinationTableIdentifier, err))
		}
		srcTables = append(srcTables, src.String())
		dstTables = append(dstTables, dst.String())
		tableMap[*src] = *dst
		if !slices.Contains(srcSchemas, src.Namespace) {
			srcSchemas = append(srcSchemas, src.Namespace)
		}
	}

	srcConn, srcClose, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, nil, h.pool, config.SourceName)
	if err != nil {
		return nil, NewInternalApiError(fmt.Errorf("failed to get source postgres connector: %w", err))
	}
	defer srcClose(ctx)

	dstConn, dstClose, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, nil, h.pool, config.DestinationName)
	if err != nil {
		return nil, NewInternalApiError(fmt.Errorf("failed to get destination postgres connector: %w", err))
	}
	defer dstClose(ctx)

	fail := func(msg string, err error) (*protos.ResetMirrorSequencesResponse, APIError) {
		slog.ErrorContext(ctx, msg, slog.Any("error", err))
		return &protos.ResetMirrorSequencesResponse{
			Ok:           false,
			ErrorMessage: fmt.Sprintf("%s: %v", msg, err),
		}, nil
	}

	srcSeqs, err := querySourceSequences(ctx, srcConn.Conn(), srcSchemas)
	if err != nil {
		return fail("failed to list source sequences", err)
	}
	srcColumnSeqs, err := queryColumnSequences(ctx, srcConn.Conn(), srcTables)
	if err != nil {
		return fail("failed to discover source column sequences", err)
	}
	srcSeqColumns := make(map[sequenceRef][]columnRef)
	for col, seqs := range srcColumnSeqs {
		for _, seq := range seqs {
			srcSeqColumns[seq] = append(srcSeqColumns[seq], col)
		}
	}

	dstExisting, err := queryExistingSequences(ctx, dstConn.Conn(), srcSchemas)
	if err != nil {
		return fail("failed to list destination sequences", err)
	}
	dstColumnSeqs, err := queryColumnSequences(ctx, dstConn.Conn(), dstTables)
	if err != nil {
		return fail("failed to discover destination column sequences", err)
	}

	targets := make(map[sequenceRef]int64)
	propose := func(seq sequenceRef, value int64) {
		if current, ok := targets[seq]; !ok || value > current {
			targets[seq] = value
		}
	}

	var warnings []string
	for _, src := range srcSeqs {
		if src.lastValue == nil {
			// never used on the source, nothing to carry over
			continue
		}
		matched := false
		if _, ok := dstExisting[src.ref]; ok {
			propose(src.ref, *src.lastValue)
			matched = true
		}
		for _, col := range srcSeqColumns[src.ref] {
			dstTable, ok := tableMap[col.Table]
			if !ok {
				continue
			}
			for _, dstSeq := range dstColumnSeqs[columnRef{dstTable, col.Column}] {
				propose(dstSeq, *src.lastValue)
				matched = true
			}
		}
		if !matched {
			warnings = append(warnings,
				fmt.Sprintf("source sequence %s has no matching sequence on the destination and was skipped", src.ref))
		}
	}

	for col, seqs := range dstColumnSeqs {
		var maxValue *int64
		if err := dstConn.Conn().QueryRow(ctx,
			fmt.Sprintf("SELECT MAX(%s)::bigint FROM %s", common.QuoteIdentifier(col.Column), col.Table.String()),
		).Scan(&maxValue); err != nil {
			return fail(fmt.Sprintf("failed to read MAX(%s) from %s", col.Column, col.Table.String()), err)
		}
		if maxValue != nil && *maxValue > 0 {
			for _, seq := range seqs {
				propose(seq, *maxValue)
			}
		}
	}

	ordered := make([]sequenceRef, 0, len(targets))
	for seq := range targets {
		ordered = append(ordered, seq)
	}
	slices.SortFunc(ordered, func(a, b sequenceRef) int {
		return strings.Compare(a.String(), b.String())
	})
	for _, seq := range ordered {
		if _, err := dstConn.Conn().Exec(ctx, "SELECT setval($1::regclass, $2, true)", seq.String(), targets[seq]); err != nil {
			return fail(fmt.Sprintf("failed to reset sequence %s", seq), err)
		}
	}

	slices.Sort(warnings)
	return &protos.ResetMirrorSequencesResponse{
		Ok:       true,
		Warnings: warnings,
	}, nil
}
