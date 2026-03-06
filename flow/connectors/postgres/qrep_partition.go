package connpostgres

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type PartitionParams struct {
	tx               pgx.Tx
	lastRangeEnd     any
	logger           log.Logger
	watermarkTable   string
	watermarkColumn  string
	numPartitions    int64
	addNullPartition bool
}

type PartitioningFunc func(context.Context, PartitionParams) ([]*protos.QRepPartition, error)

// NTileBucketPartitioningFunc is a table partition implementation that divides rows into approximately
// equal-sized partitions based on row count. It uses the NTILE window function to assign rows
// to buckets and ensures more balanced row distribution across partitions.
func NTileBucketPartitioningFunc(ctx context.Context, pp PartitionParams) ([]*protos.QRepPartition, error) {
	const queryTemplate = `SELECT bucket, MIN(%[2]s) AS start, MAX(%[2]s) AS end
		FROM (
			SELECT NTILE(%[1]d) OVER (ORDER BY %[2]s) AS bucket, %[2]s FROM %[3]s %[4]s
		) subquery
		GROUP BY bucket
		ORDER BY start`
	var whereClause string
	var queryArgs []any
	if pp.lastRangeEnd != nil {
		whereClause = fmt.Sprintf("WHERE %s > $1", pp.watermarkColumn)
		queryArgs = []any{pp.lastRangeEnd}
	}
	partitionsQuery := fmt.Sprintf(queryTemplate, pp.numPartitions, pp.watermarkColumn, pp.watermarkTable, whereClause)
	pp.logger.Info("[NTileBucketPartitioning] partitions query", slog.String("query", partitionsQuery))

	rows, err := pp.tx.Query(ctx, partitionsQuery, queryArgs...)
	if err != nil {
		return nil, shared.LogError(pp.logger, fmt.Errorf("failed to query for partitions: %w", err))
	}
	defer rows.Close()

	partitionHelper := utils.NewPartitionHelper(pp.logger)
	for rows.Next() {
		var bucket pgtype.Int8
		var start, end any
		if err := rows.Scan(&bucket, &start, &end); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		if err := partitionHelper.AddPartition(start, end); err != nil {
			return nil, fmt.Errorf("failed to add partition: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read rows: %w", err)
	}
	if pp.addNullPartition {
		partitionHelper.AddNullPartition()
	}
	return partitionHelper.GetPartitions(), nil
}

// MinMaxRangePartitioningFunc is a table partition strategy where partitions are created
// by uniformly splitting the min/max value range. Note that partition boundaries are uniform,
// but actual row distribution may be skewed due to non-uniform data distribution, gaps in the
// value range, or deleted rows.
func MinMaxRangePartitioningFunc(ctx context.Context, pp PartitionParams) ([]*protos.QRepPartition, error) {
	if pp.numPartitions <= 1 {
		return nil, fmt.Errorf("expect numPartitions to be greater than 1")
	}

	const queryTemplate = "SELECT MIN(%[2]s),MAX(%[2]s) FROM %[1]s %[3]s"
	var whereClause string
	var queryArgs []any
	if pp.lastRangeEnd != nil {
		whereClause = fmt.Sprintf("WHERE %s > $1", pp.watermarkColumn)
		queryArgs = []any{pp.lastRangeEnd}
	}
	partitionsQuery := fmt.Sprintf(queryTemplate, pp.watermarkTable, pp.watermarkColumn, whereClause)
	pp.logger.Info("[MinMaxRangePartitioning] partitions query", slog.String("query", partitionsQuery))

	var start, end any
	if err := pp.tx.QueryRow(ctx, partitionsQuery, queryArgs...).Scan(&start, &end); err != nil {
		return nil, shared.LogError(pp.logger, fmt.Errorf("failed to query for partitions: %w", err))
	}

	partitionHelper := utils.NewPartitionHelper(pp.logger)
	if err := partitionHelper.AddPartitionsWithRange(start, end, pp.numPartitions); err != nil {
		return nil, fmt.Errorf("failed to add partitions: %w", err)
	}

	// add null values partition to the end, if nulls aren't present it will be an empty partition
	// that gets skipped during replication
	if pp.addNullPartition {
		partitionHelper.AddNullPartition()
	}

	return partitionHelper.GetPartitions(), nil
}

// CTIDBlockPartitioningFunc is a table partition strategy where partitions are created by dividing table
// blocks uniformly. Note that partition boundaries (block ranges) are uniform, but actual row distribution
// may be skewed due to table bloat, deleted tuples, or uneven data distribution across blocks.
func CTIDBlockPartitioningFunc(ctx context.Context, pp PartitionParams) ([]*protos.QRepPartition, error) {
	if pp.numPartitions <= 1 {
		return nil, fmt.Errorf("expect numPartitions to be greater than 1")
	}

	isPartitioned, err := isPartitionedTable(ctx, pp.tx, pp.watermarkTable)
	if err != nil {
		return nil, err
	}
	if isPartitioned {
		blocksPerTable, err := getPartitionedTables(ctx, pp.tx, pp.watermarkTable)
		if err != nil {
			return nil, fmt.Errorf("failed to get partitioned tables: %w", err)
		}
		var totalBlocks int64
		for _, blocks := range blocksPerTable {
			totalBlocks += blocks
		}
		if totalBlocks == 0 {
			pp.logger.Warn("all child partitions are empty, returning empty partition list")
			return nil, nil
		}
		pp.logger.Info("[CTIDBlockPartitioning] partitioned table detected",
			slog.String("table", pp.watermarkTable),
			slog.Int("numPartitionedTable", len(blocksPerTable)),
			slog.Int64("totalBlocks", totalBlocks))
		return ctidPartitionsForPartitionedTable(pp, blocksPerTable, totalBlocks)
	}

	var totalBlocks pgtype.Int8
	if err := pp.tx.QueryRow(ctx,
		"SELECT (pg_relation_size(to_regclass($1)) / current_setting('block_size')::int)::bigint",
		pp.watermarkTable).Scan(&totalBlocks); err != nil {
		return nil, fmt.Errorf("failed to get relation blocks for %s: %w", pp.watermarkTable, err)
	}
	if !totalBlocks.Valid || totalBlocks.Int64 == 0 {
		pp.logger.Warn("table has 0 blocks, returning empty partition list", slog.String("table", pp.watermarkTable))
		return nil, nil
	}
	return ctidPartitionsForTable(pp, totalBlocks.Int64)
}

// ctidPartitionsForTable generates CTID block partitions for a single (non-partitioned) table.
func ctidPartitionsForTable(pp PartitionParams, totalBlocks int64) ([]*protos.QRepPartition, error) {
	if totalBlocks <= 0 {
		return nil, fmt.Errorf("invalid block count %d for table %s", totalBlocks, pp.watermarkTable)
	}

	tidCmp := func(a pgtype.TID, b pgtype.TID) int {
		if blockCmp := cmp.Compare(a.BlockNumber, b.BlockNumber); blockCmp != 0 {
			return blockCmp
		}
		return cmp.Compare(a.OffsetNumber, b.OffsetNumber)
	}

	tidInc := func(t pgtype.TID) pgtype.TID {
		if t.OffsetNumber < math.MaxUint16 {
			return pgtype.TID{BlockNumber: t.BlockNumber, OffsetNumber: t.OffsetNumber + 1, Valid: true}
		}
		return pgtype.TID{BlockNumber: t.BlockNumber + 1, OffsetNumber: 0, Valid: true}
	}

	tidRangeForPartition := func(partitionIndex int64) (pgtype.TID, pgtype.TID, bool) {
		blockStart := uint32((partitionIndex * totalBlocks) / pp.numPartitions)
		nextPartitionBlockStart := uint32(((partitionIndex + 1) * totalBlocks) / pp.numPartitions)
		if nextPartitionBlockStart <= blockStart {
			return pgtype.TID{}, pgtype.TID{}, false
		}
		tidStartInclusive := pgtype.TID{BlockNumber: blockStart, OffsetNumber: 0, Valid: true}
		tidEndInclusive := pgtype.TID{BlockNumber: nextPartitionBlockStart - 1, OffsetNumber: math.MaxUint16, Valid: true}
		return tidStartInclusive, tidEndInclusive, true
	}

	var resumeFrom pgtype.TID
	if pp.lastRangeEnd != nil {
		if lastTID, ok := pp.lastRangeEnd.(pgtype.TID); ok {
			resumeFrom = tidInc(lastTID)
		} else {
			pp.logger.Warn("Ignoring resume offset because it's not TidRange")
		}
	}

	partitionHelper := utils.NewPartitionHelper(pp.logger)
	for i := range pp.numPartitions {
		start, end, valid := tidRangeForPartition(i)
		if !valid {
			continue
		}
		if resumeFrom.Valid {
			if tidCmp(end, resumeFrom) < 0 {
				continue
			}
			if tidCmp(start, resumeFrom) < 0 {
				start = resumeFrom
			}
		}
		if err := partitionHelper.AddPartition(
			pgtype.TID{BlockNumber: start.BlockNumber, OffsetNumber: start.OffsetNumber, Valid: true},
			pgtype.TID{BlockNumber: end.BlockNumber, OffsetNumber: end.OffsetNumber, Valid: true},
		); err != nil {
			return nil, fmt.Errorf("failed to add TID partition: %w", err)
		}
	}
	return partitionHelper.GetPartitions(), nil
}

// ctidPartitionsForPartitionedTable generates snapshot partitions for a partitioned table.
// It walks child tables sequentially, greedily filling each partition with <blocksPerPartition>
// blocks before starting the next. A single snapshot partition may span multiple child tables,
// and a single child table may be split across snapshot partitions.
//
// Example: children [T1:30 blocks, T2:20 blocks, T3:10 blocks] with blocksPerPartition=25 produces:
//   - partition 1: [T1:0-24]
//   - partition 2: [T1:25-29, T2:0-19]
//   - partition 3: [T3:0-9]
func ctidPartitionsForPartitionedTable(
	pp PartitionParams,
	blocksPerTable map[string]int64,
	totalChildBlocks int64,
) ([]*protos.QRepPartition, error) {
	sortedTables := slices.Sorted(maps.Keys(blocksPerTable))
	blocksPerPartition := max(int64(1), (totalChildBlocks+pp.numPartitions-1)/pp.numPartitions) // ceiling division

	var partitions []*protos.QRepPartition
	childIdx := 0
	blockOffset := int64(0)
	for childIdx < len(sortedTables) {
		remaining := blocksPerPartition
		var ranges []*protos.ChildTableRange

		for remaining > 0 && childIdx < len(sortedTables) {
			tableName := sortedTables[childIdx]
			blocks := blocksPerTable[tableName]
			remainingBlocks := blocks - blockOffset
			consume := min(remaining, remainingBlocks)

			blockStart := uint32(blockOffset)
			blockEnd := uint32(blockOffset + consume)

			ranges = append(ranges, &protos.ChildTableRange{
				Table: tableName,
				Range: &protos.TIDPartitionRange{
					Start: &protos.TID{BlockNumber: blockStart, OffsetNumber: 0},
					End:   &protos.TID{BlockNumber: blockEnd - 1, OffsetNumber: math.MaxUint16},
				},
			})

			remaining -= consume
			blockOffset += consume
			if blockOffset >= blocks {
				childIdx++
				blockOffset = 0
			}
		}

		if len(ranges) > 0 {
			partitions = append(partitions, &protos.QRepPartition{
				PartitionId:      uuid.NewString(),
				ChildTableRanges: ranges,
			})
		}
	}

	pp.logger.Info("[CTIDBlockPartitioning] generated partitions for partitioned table",
		slog.Int("numPartitions", len(partitions)),
		slog.Int64("blocksPerPartition", blocksPerPartition),
		slog.Int64("totalBlocks", totalChildBlocks))

	return partitions, nil
}

func isPartitionedTable(ctx context.Context, tx pgx.Tx, table string) (bool, error) {
	var relkind string
	err := tx.QueryRow(ctx,
		`SELECT c.relkind::text FROM pg_class c WHERE c.oid = to_regclass($1)`,
		table).Scan(&relkind)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, fmt.Errorf("table %s not found in pg_class", table)
		}
		return false, fmt.Errorf("failed to check relkind for %s: %w", table, err)
	}
	return relkind == "p", nil
}

// getPartitionedTables returns a map of partitioned child table names to their block counts,
// recursively handle multi-level partitioned tables.
func getPartitionedTables(ctx context.Context, tx pgx.Tx, table string) (map[string]int64, error) {
	rows, err := tx.Query(ctx, `
		SELECT format('%I.%I', n.nspname, c.relname), c.relkind::text,
			(pg_relation_size(c.oid) / current_setting('block_size')::int)::bigint
		FROM pg_inherits i
		JOIN pg_class c ON i.inhrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE i.inhparent = to_regclass($1)
		ORDER BY c.relname
	`, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query child tables for %s: %w", table, err)
	}
	defer rows.Close()

	type tableInfo struct {
		name   string
		kind   string
		blocks int64
	}
	var tableInfos []tableInfo
	for rows.Next() {
		var info tableInfo
		if err := rows.Scan(&info.name, &info.kind, &info.blocks); err != nil {
			return nil, fmt.Errorf("failed to scan child table: %w", err)
		}
		tableInfos = append(tableInfos, info)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	blocksPerPartitionedTable := make(map[string]int64)
	for _, info := range tableInfos {
		if info.kind == "p" {
			leaveTables, err := getPartitionedTables(ctx, tx, info.name)
			if err != nil {
				return nil, fmt.Errorf("failed to get partitions of %s: %w", info.name, err)
			}
			maps.Copy(blocksPerPartitionedTable, leaveTables)
		} else if info.blocks > 0 { // skips empty table
			blocksPerPartitionedTable[info.name] = info.blocks
		}
	}
	return blocksPerPartitionedTable, nil
}

// ComputeNumPartitions computes the number of partitions given desired number of rows
// per partition, with automatic adjustment to respect the maximum partition limit.
// TODO: use estimated row count for partitioned/inherited tables as well
func ComputeNumPartitions(ctx context.Context, pp PartitionParams, numRowsPerPartition int64) (int64, error) {
	const preciseCountTemplate = "SELECT COUNT(*) FROM %s %s"
	// Use reltuples/relpages density multiplied by the current on-disk page count
	// for a more accurate estimate than just reltuples (this is what the planner uses,
	// see https://www.citusdata.com/blog/2016/10/12/count-performance/#dup_counts_estimated_full).
	// For inherited/partitioned tables pg_relation_size, reltuples, and relpages
	// only reflect the parent table, so we return 0 and fall back to precise count query.
	const estimatedCountTemplate = `SELECT CASE
		WHEN c.relhassubclass THEN 0
		WHEN c.reltuples >= 0 AND c.relpages > 0 THEN
			(c.reltuples / c.relpages * (pg_relation_size(c.oid) / current_setting('block_size')::integer))::bigint
		ELSE c.reltuples::bigint
		END
		FROM pg_class c WHERE c.oid = to_regclass($1)`

	var totalRows pgtype.Int8
	var whereClause string
	var queryArgs []any

	if pp.lastRangeEnd == nil {
		pp.logger.Info("fetch estimated row count", slog.String("query", estimatedCountTemplate))
		if err := pp.tx.QueryRow(ctx, estimatedCountTemplate, pp.watermarkTable).Scan(&totalRows); err != nil {
			return 0, fmt.Errorf("failed to query for estimated row count: %w", err)
		}
	} else {
		whereClause = fmt.Sprintf("WHERE %s > $1", pp.watermarkColumn)
		queryArgs = []any{pp.lastRangeEnd}
	}

	// reltuples is -1 if the table has never been analyzed
	// reltuples is 0 if:
	//   - table is new and stats is stale; or
	//   - table is inherited/partitioned (see query above); or
	//   - polling-based flows (estimated row count is skipped)
	// In all cases, fall back to precise count query
	if totalRows.Int64 <= 0 {
		countQuery := fmt.Sprintf(preciseCountTemplate, pp.watermarkTable, whereClause)
		pp.logger.Info("fetch row count", slog.String("query", countQuery))
		if err := pp.tx.QueryRow(ctx, countQuery, queryArgs...).Scan(&totalRows); err != nil {
			return 0, fmt.Errorf("failed to query for precise row count: %w", err)
		}
	}
	if totalRows.Int64 <= 0 {
		pp.logger.Warn("no records to replicate, returning")
		return 0, nil
	}

	adjustedPartitions := shared.AdjustNumPartitions(totalRows.Int64, numRowsPerPartition)
	pp.logger.Info("[postgres] partition details",
		slog.Int64("totalRows", totalRows.Int64),
		slog.Int64("desiredNumRowsPerPartition", numRowsPerPartition),
		slog.Int64("adjustedNumPartitions", adjustedPartitions.AdjustedNumPartitions),
		slog.Int64("adjustedNumRowsPerPartition", adjustedPartitions.AdjustedNumRowsPerPartition))
	numPartitions := adjustedPartitions.AdjustedNumPartitions

	return numPartitions, nil
}
