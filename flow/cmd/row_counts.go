package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/sync/errgroup"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

const (
	// Tables larger than this get approximate counts via reltuples/relpages
	largeTableThresholdBytes = 10 * 1024 * 1024 * 1024 // 10GB
	// Per-table statement timeout for COUNT(*)
	countTimeoutSeconds = 30
)

func (h *FlowRequestHandler) GetMirrorRowCounts(
	ctx context.Context,
	req *protos.RowCountRequest,
) (*protos.RowCountResponse, APIError) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	// Validate mirror exists and is CDC
	isCdc, err := h.isCDCFlow(ctx, req.FlowJobName)
	if err != nil {
		return nil, NewInternalApiError(fmt.Errorf("unable to check flow type: %w", err))
	}
	if !isCdc {
		return nil, NewInvalidArgumentApiError(fmt.Errorf("row count validation is only supported for CDC mirrors"))
	}

	// Get flow config to extract table mappings and peer names
	config, err := h.getFlowConfigFromCatalog(ctx, req.FlowJobName)
	if err != nil {
		return nil, NewInternalApiError(fmt.Errorf("unable to get flow config: %w", err))
	}

	// Verify both peers are Postgres
	srcType, err := connectors.LoadPeerType(ctx, h.pool, config.SourceName)
	if err != nil {
		return nil, NewInternalApiError(fmt.Errorf("unable to load source peer type: %w", err))
	}
	dstType, err := connectors.LoadPeerType(ctx, h.pool, config.DestinationName)
	if err != nil {
		return nil, NewInternalApiError(fmt.Errorf("unable to load destination peer type: %w", err))
	}
	if srcType != protos.DBType_POSTGRES {
		return nil, NewInvalidArgumentApiError(fmt.Errorf("source peer %s is not PostgreSQL", config.SourceName))
	}
	if dstType != protos.DBType_POSTGRES {
		return nil, NewInvalidArgumentApiError(fmt.Errorf("destination peer %s is not PostgreSQL", config.DestinationName))
	}

	// Filter table mappings if source_tables filter is specified
	tableMappings := config.TableMappings
	if len(req.SourceTables) > 0 {
		filterSet := make(map[string]struct{}, len(req.SourceTables))
		for _, t := range req.SourceTables {
			filterSet[t] = struct{}{}
		}
		filtered := make([]*protos.TableMapping, 0, len(req.SourceTables))
		for _, tm := range tableMappings {
			if _, ok := filterSet[tm.SourceTableIdentifier]; ok {
				filtered = append(filtered, tm)
			}
		}
		tableMappings = filtered
	}

	if len(tableMappings) == 0 {
		return &protos.RowCountResponse{TableCounts: nil}, nil
	}

	// Open connections to source and destination
	srcConn, srcClose, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, nil, h.pool, config.SourceName)
	if err != nil {
		return nil, NewInternalApiError(fmt.Errorf("failed to connect to source peer: %w", err))
	}
	defer srcClose(ctx)

	dstConn, dstClose, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, nil, h.pool, config.DestinationName)
	if err != nil {
		return nil, NewInternalApiError(fmt.Errorf("failed to connect to destination peer: %w", err))
	}
	defer dstClose(ctx)

	// Build result slice
	results := make([]*protos.TableRowCount, len(tableMappings))
	for i, tm := range tableMappings {
		results[i] = &protos.TableRowCount{
			SourceTable:      tm.SourceTableIdentifier,
			DestinationTable: tm.DestinationTableIdentifier,
		}
	}

	// Query source and destination in parallel,
	// but serialize queries within each peer since pgx.Conn is not concurrent-safe.
	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		for i, tm := range tableMappings {
			if gCtx.Err() != nil {
				return gCtx.Err()
			}
			count, isApprox, err := getTableRowCount(gCtx, srcConn.Conn(), tm.SourceTableIdentifier)
			if err != nil {
				slog.WarnContext(gCtx, "failed to get source row count",
					slog.String("table", tm.SourceTableIdentifier), slog.Any("error", err))
				results[i].SourceCount = -1
			} else {
				results[i].SourceCount = count
				results[i].SourceIsApproximate = isApprox
			}
		}
		return nil
	})

	g.Go(func() error {
		for i, tm := range tableMappings {
			if gCtx.Err() != nil {
				return gCtx.Err()
			}
			count, isApprox, err := getTableRowCount(gCtx, dstConn.Conn(), tm.DestinationTableIdentifier)
			if err != nil {
				slog.WarnContext(gCtx, "failed to get destination row count",
					slog.String("table", tm.DestinationTableIdentifier), slog.Any("error", err))
				results[i].DestinationCount = -1
			} else {
				results[i].DestinationCount = count
				results[i].DestinationIsApproximate = isApprox
			}
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, NewInternalApiError(fmt.Errorf("error getting row counts: %w", err))
	}

	return &protos.RowCountResponse{TableCounts: results}, nil
}

// getTableRowCount gets the row count for a table. For tables >= 10GB it returns
// an approximate count using reltuples/relpages. For smaller tables it runs COUNT(*)
// with a statement timeout. Returns -1 if the count query times out.
func getTableRowCount(ctx context.Context, conn *pgx.Conn, tableIdentifier string) (int64, bool, error) {
	parsed, err := common.ParseTableIdentifier(tableIdentifier)
	if err != nil {
		return -1, false, fmt.Errorf("failed to parse table identifier %s: %w", tableIdentifier, err)
	}
	qualifiedName := parsed.String()

	// Get table size to decide strategy
	var tableSizeBytes pgtype.Int8
	if err := conn.QueryRow(ctx,
		"SELECT pg_total_relation_size(to_regclass($1))", qualifiedName,
	).Scan(&tableSizeBytes); err != nil {
		return -1, false, fmt.Errorf("failed to get table size for %s: %w", tableIdentifier, err)
	}
	if !tableSizeBytes.Valid {
		return -1, false, fmt.Errorf("table %s does not exist or size is null", tableIdentifier)
	}

	if tableSizeBytes.Int64 >= largeTableThresholdBytes {
		return getApproximateRowCount(ctx, conn, qualifiedName)
	}
	return getExactRowCount(ctx, conn, qualifiedName)
}

// getApproximateRowCount uses the reltuples/relpages density formula (same as the PG planner).
func getApproximateRowCount(ctx context.Context, conn *pgx.Conn, qualifiedName string) (int64, bool, error) {
	const estimatedCountQuery = `SELECT
		CASE
			WHEN c.reltuples >= 0 AND c.relpages > 0 AND NOT c.relhassubclass
				THEN (c.reltuples::numeric / c.relpages *
					(pg_relation_size(c.oid) / current_setting('block_size')::integer))::bigint
			ELSE -1
		END
	FROM pg_class c WHERE c.oid = to_regclass($1)`

	var count pgtype.Int8
	if err := conn.QueryRow(ctx, estimatedCountQuery, qualifiedName).Scan(&count); err != nil {
		return -1, true, fmt.Errorf("failed to get approximate count for %s: %w", qualifiedName, err)
	}
	if !count.Valid || count.Int64 < 0 {
		return -1, true, nil
	}
	return count.Int64, true, nil
}

// getExactRowCount runs COUNT(*) with a statement timeout. Returns -1 if timed out.
func getExactRowCount(ctx context.Context, conn *pgx.Conn, qualifiedName string) (int64, bool, error) {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return -1, false, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	timeoutMs := countTimeoutSeconds * 1000
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL statement_timeout = '%d'", timeoutMs)); err != nil {
		return -1, false, fmt.Errorf("failed to set statement timeout: %w", err)
	}

	var count int64
	if err := tx.QueryRow(ctx, "SELECT COUNT(*) FROM "+qualifiedName).Scan(&count); err != nil {
		if shared.IsSQLStateError(err, pgerrcode.QueryCanceled) {
			return -1, false, nil
		}
		return -1, false, fmt.Errorf("failed to count rows in %s: %w", qualifiedName, err)
	}

	return count, false, nil
}
