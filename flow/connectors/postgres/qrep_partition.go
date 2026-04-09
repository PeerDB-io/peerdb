package connpostgres

import (
	"cmp"
	"context"
	"errors"
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
	if pp.numPartitions <= 0 {
		return nil, fmt.Errorf("expect numPartitions to be greater than 0")
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
	if pp.numPartitions <= 0 {
		return nil, fmt.Errorf("expect numPartitions to be greater than 0")
	}

	tc, err := classifyTable(ctx, pp.tx, pp.watermarkTable)
	if err != nil {
		return nil, err
	}
	switch {
	case tc.relkind == "p":
		blocksPerTable, err := getPartitionedTables(ctx, pp.tx, tc.qualifiedName)
		if err != nil {
			return nil, fmt.Errorf("failed to get child partitioned tables: %w", err)
		}
		return ctidPartitionsForChildTables(pp, blocksPerTable)
	case tc.relhassubclass:
		blocksPerTable, err := getInheritedTables(ctx, pp.tx, tc.qualifiedName)
		if err != nil {
			return nil, fmt.Errorf("failed to get child inherited tables: %w", err)
		}
		return ctidPartitionsForChildTables(pp, blocksPerTable)
	default:
		return ctidPartitionsForTable(ctx, pp)
	}
}

// ctidPartitionsForTable generates CTID block partitions for a single (non-partitioned) table.
func ctidPartitionsForTable(ctx context.Context, pp PartitionParams) ([]*protos.QRepPartition, error) {
	totalBlocks, err := getTableBlockCount(ctx, pp.tx, pp.watermarkTable)
	if err != nil {
		return nil, err
	}
	if totalBlocks == 0 {
		pp.logger.Warn("table has 0 blocks, returning empty partition list", slog.String("table", pp.watermarkTable))
		return nil, nil
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

// ctidPartitionsForChildTables discovers child tables via the provided discovery function,
// then generates snapshot partitions using a greedy algorithm.
// It walks tables (sorted by name) sequentially, greedily filling each partition with
// <blocksPerPartition> blocks before starting the next. A single snapshot partition may
// span multiple tables, and a single table may be split across snapshot partitions.
//
// Example: tables [T1:30 blocks, T2:20 blocks, T3:10 blocks] with blocksPerPartition=25 produces:
//   - partition 1: [T1:0-24]
//   - partition 2: [T1:25-29, T2:0-19]
//   - partition 3: [T3:0-9]
func ctidPartitionsForChildTables(
	pp PartitionParams,
	blocksPerTable map[string]int64,
) ([]*protos.QRepPartition, error) {
	var totalBlocks int64
	for _, blocks := range blocksPerTable {
		totalBlocks += blocks
	}
	if totalBlocks == 0 {
		pp.logger.Warn("zero non-empty child tables found")
		return nil, nil
	}

	pp.logger.Info("child tables detected",
		slog.String("table", pp.watermarkTable),
		slog.Int("numChildTables", len(blocksPerTable)),
		slog.Int64("totalBlocks", totalBlocks))

	sortedTables := slices.Sorted(maps.Keys(blocksPerTable))
	blocksPerPartition := max(int64(1), shared.DivCeil(totalBlocks, pp.numPartitions))

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
				Start: blockStart,
				End:   blockEnd - 1,
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

	pp.logger.Info("generated partitions for child tables",
		slog.Int("numPartitions", len(partitions)),
		slog.Int64("blocksPerPartition", blocksPerPartition),
		slog.Int64("totalBlocks", totalBlocks))

	return partitions, nil
}

func getTableBlockCount(ctx context.Context, tx pgx.Tx, table string) (int64, error) {
	var blocks int64
	if err := tx.QueryRow(ctx,
		"SELECT (pg_relation_size(to_regclass($1)) / current_setting('block_size')::int)::bigint",
		table).Scan(&blocks); err != nil {
		return 0, fmt.Errorf("failed to get block count for %s: %w", table, err)
	}
	return blocks, nil
}

type tableClassification struct {
	qualifiedName  string
	relkind        string
	relhassubclass bool
}

func classifyTable(ctx context.Context, tx pgx.Tx, table string) (tableClassification, error) {
	var classification tableClassification
	err := tx.QueryRow(ctx,
		// We fetch the qualified name from pg_class instead of using the input directly because
		// the input table name is quoted; and we want the unquoted canonical form for uniformity
		`SELECT format('%s.%s', n.nspname, c.relname), c.relkind::text, c.relhassubclass
		 FROM pg_class c
		 JOIN pg_namespace n ON c.relnamespace = n.oid
		 WHERE c.oid = to_regclass($1)`,
		table).Scan(&classification.qualifiedName, &classification.relkind, &classification.relhassubclass)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return classification, fmt.Errorf("table %s not found in pg_class", table)
		}
		return classification, fmt.Errorf("failed to classify table %s: %w", table, err)
	}
	return classification, nil
}

// getPartitionedTables returns a map of partitioned child table names to their block counts,
// recursively handle multi-level partitioned tables.
func getPartitionedTables(ctx context.Context, tx pgx.Tx, table string) (map[string]int64, error) {
	rows, err := tx.Query(ctx, `
		SELECT format('%s.%s', n.nspname, c.relname), c.relkind::text,
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

// getInheritedTables returns a map of table names to their block counts for an inherited
// table hierarchy. Unlike partitioned tables, the parent itself stores data and is included.
func getInheritedTables(ctx context.Context, tx pgx.Tx, table string) (map[string]int64, error) {
	blocksPerTable := make(map[string]int64)

	numBlocks, err := getTableBlockCount(ctx, tx, table)
	if err != nil {
		return nil, err
	}
	if numBlocks > 0 {
		blocksPerTable[table] = numBlocks
	}

	rows, err := tx.Query(ctx, `
		SELECT format('%s.%s', n.nspname, c.relname), c.relhassubclass,
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
		name        string
		hasSubclass bool
		blocks      int64
	}
	var children []tableInfo
	for rows.Next() {
		var info tableInfo
		if err := rows.Scan(&info.name, &info.hasSubclass, &info.blocks); err != nil {
			return nil, fmt.Errorf("failed to scan child table: %w", err)
		}
		children = append(children, info)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, child := range children {
		if child.blocks > 0 {
			blocksPerTable[child.name] = child.blocks
		}
		if child.hasSubclass {
			childTables, err := getInheritedTables(ctx, tx, child.name)
			if err != nil {
				return nil, fmt.Errorf("failed to get inherited tables of %s: %w", child.name, err)
			}
			maps.Copy(blocksPerTable, childTables)
		}
	}

	return blocksPerTable, nil
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
