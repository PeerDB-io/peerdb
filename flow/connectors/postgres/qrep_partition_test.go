package connpostgres

import (
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

//nolint:govet // field alignment not important
type testCase struct {
	name                  string
	config                *protos.QRepConfig
	last                  *protos.QRepPartition
	expectedNumPartitions int
	wantErr               bool
}

func newTestCaseForNumRows(schema string, name string, rows uint32, expectedNum int) *testCase {
	schemaQualifiedTable := schema + ".test"
	query := fmt.Sprintf(
		`SELECT * FROM %s WHERE "from" >= {{.start}} AND "from" < {{.end}}`,
		schemaQualifiedTable)
	return &testCase{
		name: name,
		config: &protos.QRepConfig{
			FlowJobName:         "test_flow_job",
			NumRowsPerPartition: rows,
			Query:               query,
			WatermarkTable:      schemaQualifiedTable,
			WatermarkColumn:     "from",
			InitialCopyOnly:     true,
		},
		expectedNumPartitions: expectedNum,
	}
}

func newTestCaseForNumRowsWithNulls(schema string, name string, rows uint32, expectedNum int) *testCase {
	tc := newTestCaseForNumRows(schema, name, rows, expectedNum)
	tc.config.AddNullPartition = true
	return tc
}

func newTestCaseForCTID(schema string, name string, rows uint32, expectedNum int) *testCase {
	schemaQualifiedTable := schema + ".test"
	query := fmt.Sprintf(
		`SELECT * FROM %s WHERE "from" >= {{.start}} AND "from" < {{.end}}`,
		schemaQualifiedTable)
	return &testCase{
		name: name,
		config: &protos.QRepConfig{
			FlowJobName:         "test_flow_job",
			NumRowsPerPartition: rows,
			Query:               query,
			WatermarkTable:      schemaQualifiedTable,
			WatermarkColumn:     ctidColumnName,
		},
		expectedNumPartitions: expectedNum,
	}
}

func setupTestSchema(t *testing.T) (string, *pgx.Conn) {
	t.Helper()
	catalogConnStr := internal.GetCatalogConnectionStringFromEnv(t.Context())

	config, err := pgx.ParseConfig(catalogConnStr)
	if err != nil {
		t.Fatalf("Failed to parse config: %v", err)
	}

	tunnel, err := utils.NewSSHTunnel(t.Context(), nil)
	if err != nil {
		t.Fatalf("Failed to create tunnel: %v", err)
	}
	t.Cleanup(func() { tunnel.Close() })

	conn, err := NewPostgresConnFromConfig(t.Context(), config, "", nil, tunnel)
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}
	t.Cleanup(func() { conn.Close(t.Context()) })

	//nolint:gosec // Generate a random schema name, number has no cryptographic significance
	schemaName := fmt.Sprintf("test_%d", rand.Uint64())

	_, err = conn.Exec(t.Context(), fmt.Sprintf(`CREATE SCHEMA %s;`, schemaName))
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}
	t.Cleanup(func() {
		if _, err := conn.Exec(t.Context(), fmt.Sprintf(`DROP SCHEMA %s CASCADE;`, schemaName)); err != nil {
			t.Logf("Failed to drop schema: %v", err)
		}
	})

	_, err = conn.Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.test (
			id SERIAL PRIMARY KEY,
			value INT NOT NULL,
			"from" TIMESTAMP
		)
	`, schemaName))
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	return schemaName, conn
}

func TestGetQRepPartitions(t *testing.T) {
	t.Parallel()
	schemaName, conn := setupTestSchema(t)

	// from 2010 Jan 1 10:00 AM UTC to 2010 Jan 30 10:00 AM UTC
	numRows := prepareTestData(t, conn, schemaName, false)

	// Define the test cases
	testCases := []*testCase{
		newTestCaseForNumRows(
			schemaName,
			"ensure all rows are in 1 partition if num_rows_per_partition is size of table",
			uint32(numRows),
			1,
		),
		newTestCaseForNumRows(
			schemaName,
			"ensure all rows are in 2 partitions if num_rows_per_partition is half the size of table",
			uint32(numRows)/2,
			2,
		),
		newTestCaseForNumRows(
			schemaName,
			"ensure all rows are in 3 partitions if num_rows_per_partition is 1/3 the size of table",
			uint32(numRows)/3,
			3,
		),
		// 30 rows / 7 rows per partition = DivCeil(30, 7) = 5 partitions
		newTestCaseForNumRows(
			schemaName,
			"ensure all rows are in 5 partitions if num_rows_per_partition is 1/4 the size of table",
			uint32(numRows)/4,
			5,
		),
		newTestCaseForCTID(
			schemaName,
			"ensure all rows are in 1 partition if num_rows_per_partition is size of table",
			uint32(numRows),
			1,
		),
		newTestCaseForCTID(
			schemaName,
			"ensure all rows are in 2 partitions if num_rows_per_partition is half the size of table",
			uint32(numRows)/2,
			2,
		),
		newTestCaseForCTID(
			schemaName,
			"ensure all rows are in 3 partitions if num_rows_per_partition is 1/3 the size of table",
			uint32(numRows)/3,
			3,
		),
		// 30 rows / 7 rows per partition = DivCeil(30, 7) = 5 partitions
		newTestCaseForCTID(
			schemaName,
			"ensure all rows are in 5 partitions if num_rows_per_partition is 1/4 the size of table",
			uint32(numRows)/4,
			5,
		),
	}

	c := &PostgresConnector{
		conn:   conn,
		logger: log.NewStructuredLogger(slog.With(slog.String(string(shared.FlowNameKey), "testGetQRepPartitions"))),
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := c.GetQRepPartitions(t.Context(), tc.config, tc.last)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetQRepPartitions() error = %v, wantErr %v", err, tc.wantErr)
			}

			if tc.wantErr {
				return
			}

			expected := tc.expectedNumPartitions
			assert.Len(t, got, expected)
		})
	}
}

func TestGetQRepPartitionsWithNulls(t *testing.T) {
	t.Parallel()
	schemaName, conn := setupTestSchema(t)

	// 30 non-null rows + 12 null rows = 42 total
	numRows := prepareTestData(t, conn, schemaName, true)

	testCases := []*testCase{
		newTestCaseForNumRowsWithNulls(
			schemaName,
			"1 data partition + 1 null partition",
			uint32(numRows),
			2,
		),
		newTestCaseForNumRowsWithNulls(
			schemaName,
			"2 data partitions + 1 null partition",
			uint32(numRows)/2,
			3,
		),
		newTestCaseForNumRowsWithNulls(
			schemaName,
			"3 data partitions + 1 null partition",
			uint32(numRows)/3,
			4,
		),
		// NTILE(5) groups 12 nulls into the last bucket along with some timestamps,
		// producing 4 distinct timestamp ranges + 1 explicit null partition = 5 total
		newTestCaseForNumRowsWithNulls(
			schemaName,
			"4 data partitions + 1 null partition when 1/4 table size",
			uint32(numRows)/4,
			5,
		),
	}

	c := &PostgresConnector{
		conn:   conn,
		logger: log.NewStructuredLogger(slog.With(slog.String(string(shared.FlowNameKey), "testGetQRepPartitionsWithNulls"))),
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := c.GetQRepPartitions(t.Context(), tc.config, tc.last)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetQRepPartitions() error = %v, wantErr %v", err, tc.wantErr)
			}

			if tc.wantErr {
				return
			}

			assert.Len(t, got, tc.expectedNumPartitions)
		})
	}
}

func TestCTIDPartitioningOnPartitionedTable(t *testing.T) {
	t.Parallel()
	connStr := internal.GetCatalogConnectionStringFromEnv(t.Context())

	config, err := pgx.ParseConfig(connStr)
	if err != nil {
		t.Fatalf("Failed to parse config: %v", err)
	}

	tunnel, err := utils.NewSSHTunnel(t.Context(), nil)
	if err != nil {
		t.Fatalf("Failed to create tunnel: %v", err)
	}
	defer tunnel.Close()

	conn, err := NewPostgresConnFromConfig(t.Context(), config, "", nil, tunnel)
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close(t.Context())

	//nolint:gosec
	schemaName := fmt.Sprintf("test_%d", rand.Uint64())
	_, err = conn.Exec(t.Context(), "CREATE SCHEMA "+schemaName)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}
	defer func() {
		_, _ = conn.Exec(t.Context(), fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schemaName))
	}()

	parentTable := schemaName + ".partitioned_test"
	_, err = conn.Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL,
			partition_key INT NOT NULL,
			value TEXT,
			PRIMARY KEY (partition_key, id)
		) PARTITION BY RANGE (partition_key)
	`, parentTable))
	if err != nil {
		t.Fatalf("Failed to create partitioned table: %v", err)
	}

	childTables := make([]string, 4)
	for i := range 4 {
		childTables[i] = fmt.Sprintf("%s.child_%d", schemaName, i)
		lo := i * 25
		hi := (i + 1) * 25
		_, err = conn.Exec(t.Context(), fmt.Sprintf(
			`CREATE TABLE %s PARTITION OF %s FOR VALUES FROM (%d) TO (%d)`,
			childTables[i], parentTable, lo, hi))
		if err != nil {
			t.Fatalf("Failed to create child partition %d: %v", i, err)
		}
	}

	for i := range 4 {
		for j := range 25 {
			_, err = conn.Exec(t.Context(), fmt.Sprintf(
				`INSERT INTO %s (partition_key, value) VALUES ($1, $2)`, parentTable),
				i*25+j, fmt.Sprintf("val_%d_%d", i, j))
			if err != nil {
				t.Fatalf("Failed to insert row: %v", err)
			}
		}
	}

	// pg_relation_size on parent should be 0
	var parentSize int64
	err = conn.QueryRow(t.Context(),
		`SELECT pg_relation_size(to_regclass($1))`, parentTable).Scan(&parentSize)
	require.NoError(t, err)
	require.Equal(t, int64(0), parentSize, "parent partitioned table should have 0 relation size")

	// pg_relation_size on each child should be > 0
	for _, child := range childTables {
		var childSize int64
		err = conn.QueryRow(t.Context(),
			`SELECT pg_relation_size(to_regclass($1))`, child).Scan(&childSize)
		require.NoError(t, err)
		require.Positive(t, childSize, "child table %s should have non-zero relation size", child)
	}

	// CTID-based queries should work on each child
	for i, child := range childTables {
		var count int64
		err = conn.QueryRow(t.Context(), fmt.Sprintf(
			`SELECT COUNT(*) FROM %s WHERE ctid >= '(0,0)'`, child)).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, int64(25), count, "child %d should have 25 rows accessible via CTID", i)
	}

	// GetQRepPartitions on the parent table should detect children and produce
	// CTID partitions with ChildTableRanges set to child table names.
	c := &PostgresConnector{
		connStr: connStr,
		Config:  &protos.PostgresConfig{},
		conn:    conn,
		logger:  log.NewStructuredLogger(slog.With(slog.String(string(shared.FlowNameKey), "testCTIDPartitioned"))),
	}
	query := fmt.Sprintf(`SELECT * FROM %s WHERE ctid BETWEEN {{.start}} AND {{.end}}`, parentTable)
	partitions, err := c.GetQRepPartitions(t.Context(), &protos.QRepConfig{
		FlowJobName:           "test_ctid_partitioned",
		NumRowsPerPartition:   10,
		NumPartitionsOverride: 8,
		Query:                 query,
		WatermarkTable:        parentTable,
		WatermarkColumn:       "ctid",
		Env:                   map[string]string{"PEERDB_POSTGRES_APPLY_CTID_BLOCK_PARTITIONING_OVERRIDE": "true"},
	}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, partitions, "should produce partitions for partitioned parent table")

	childTableCounts := make(map[string]int)
	for _, p := range partitions {
		require.NotEmpty(t, p.ChildTableRanges, "partitions from partitioned table must have ChildTableRanges")
		for _, ctr := range p.ChildTableRanges {
			require.NotNil(t, ctr.Range, "each ChildTableRange must have a TID range")
			require.NotEmpty(t, ctr.Table, "each ChildTableRange must name a child table")
			childTableCounts[ctr.Table]++
		}
	}

	// Every non-empty child should appear in at least one ChildTableRange
	require.Len(t, childTableCounts, 4, "should have ranges for all 4 child tables")
	for _, child := range childTables {
		require.Positive(t, childTableCounts[child], "child %s should appear in at least one partition", child)
	}
}

func TestCTIDPartitioningOnMultiLevelPartitionedTable(t *testing.T) {
	t.Parallel()
	connStr := internal.GetCatalogConnectionStringFromEnv(t.Context())

	config, err := pgx.ParseConfig(connStr)
	if err != nil {
		t.Fatalf("Failed to parse config: %v", err)
	}

	tunnel, err := utils.NewSSHTunnel(t.Context(), nil)
	if err != nil {
		t.Fatalf("Failed to create tunnel: %v", err)
	}
	defer tunnel.Close()

	conn, err := NewPostgresConnFromConfig(t.Context(), config, "", nil, tunnel)
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close(t.Context())

	//nolint:gosec
	schemaName := fmt.Sprintf("test_%d", rand.Uint64())
	_, err = conn.Exec(t.Context(), "CREATE SCHEMA "+schemaName)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}
	defer func() {
		_, _ = conn.Exec(t.Context(), fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schemaName))
	}()

	// Root table partitioned by region
	rootTable := schemaName + ".multi_level"
	_, err = conn.Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL,
			region INT NOT NULL,
			category INT NOT NULL,
			value TEXT,
			PRIMARY KEY (region, category, id)
		) PARTITION BY RANGE (region)
	`, rootTable))
	require.NoError(t, err)

	// Two mid-level partitions, each sub-partitioned by category
	for r := range 2 {
		mid := fmt.Sprintf("%s.region_%d", schemaName, r)
		_, err = conn.Exec(t.Context(), fmt.Sprintf(
			`CREATE TABLE %s PARTITION OF %s FOR VALUES FROM (%d) TO (%d) PARTITION BY RANGE (category)`,
			mid, rootTable, r*50, (r+1)*50))
		require.NoError(t, err)

		// Three leaf partitions per mid-level
		for c := range 3 {
			leaf := fmt.Sprintf("%s.region_%d_cat_%d", schemaName, r, c)
			_, err = conn.Exec(t.Context(), fmt.Sprintf(
				`CREATE TABLE %s PARTITION OF %s FOR VALUES FROM (%d) TO (%d)`,
				leaf, mid, c*34, (c+1)*34))
			require.NoError(t, err)
		}
	}

	// 6 leaf tables total, insert 20 rows into each
	expectedLeaves := make([]string, 0, 6)
	for r := range 2 {
		for c := range 3 {
			leaf := fmt.Sprintf("%s.region_%d_cat_%d", schemaName, r, c)
			expectedLeaves = append(expectedLeaves, leaf)
			for j := range 20 {
				_, err = conn.Exec(t.Context(), fmt.Sprintf(
					`INSERT INTO %s (region, category, value) VALUES ($1, $2, $3)`, rootTable),
					r*50, c*34+j%34, fmt.Sprintf("v_%d_%d_%d", r, c, j))
				require.NoError(t, err)
			}
		}
	}

	// Root table should have 0 blocks, mid-level tables should also have 0 blocks
	var rootSize int64
	err = conn.QueryRow(t.Context(),
		`SELECT pg_relation_size(to_regclass($1))`, rootTable).Scan(&rootSize)
	require.NoError(t, err)
	require.Equal(t, int64(0), rootSize)

	for r := range 2 {
		var midSize int64
		mid := fmt.Sprintf("%s.region_%d", schemaName, r)
		err = conn.QueryRow(t.Context(),
			`SELECT pg_relation_size(to_regclass($1))`, mid).Scan(&midSize)
		require.NoError(t, err)
		require.Equal(t, int64(0), midSize, "mid-level partition %s should have 0 blocks", mid)
	}

	c := &PostgresConnector{
		connStr: connStr,
		Config:  &protos.PostgresConfig{},
		conn:    conn,
		logger:  log.NewStructuredLogger(slog.With(slog.String(string(shared.FlowNameKey), "testMultiLevel"))),
	}
	query := fmt.Sprintf(`SELECT * FROM %s WHERE ctid BETWEEN {{.start}} AND {{.end}}`, rootTable)
	partitions, err := c.GetQRepPartitions(t.Context(), &protos.QRepConfig{
		FlowJobName:           "test_ctid_multi_level",
		NumRowsPerPartition:   10,
		NumPartitionsOverride: 12,
		Query:                 query,
		WatermarkTable:        rootTable,
		WatermarkColumn:       "ctid",
		Env:                   map[string]string{"PEERDB_POSTGRES_APPLY_CTID_BLOCK_PARTITIONING_OVERRIDE": "true"},
	}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, partitions)

	childTableCounts := make(map[string]int)
	for _, p := range partitions {
		require.NotEmpty(t, p.ChildTableRanges)
		for _, ctr := range p.ChildTableRanges {
			require.NotNil(t, ctr.Range)
			require.NotEmpty(t, ctr.Table)
			childTableCounts[ctr.Table]++
		}
	}

	// All 6 leaf tables should appear, no mid-level tables
	require.Len(t, childTableCounts, 6, "should have ranges for all 6 leaf tables")
	for _, leaf := range expectedLeaves {
		require.Positive(t, childTableCounts[leaf], "leaf %s should appear in at least one partition", leaf)
	}
	for r := range 2 {
		mid := fmt.Sprintf("%s.region_%d", schemaName, r)
		require.Zero(t, childTableCounts[mid], "mid-level partition %s should NOT appear in child table ranges", mid)
	}
}

func TestCTIDPartitioningOnEmptyPartitionedTable(t *testing.T) {
	t.Parallel()
	connStr := internal.GetCatalogConnectionStringFromEnv(t.Context())

	config, err := pgx.ParseConfig(connStr)
	if err != nil {
		t.Fatalf("Failed to parse config: %v", err)
	}

	tunnel, err := utils.NewSSHTunnel(t.Context(), nil)
	if err != nil {
		t.Fatalf("Failed to create tunnel: %v", err)
	}
	defer tunnel.Close()

	conn, err := NewPostgresConnFromConfig(t.Context(), config, "", nil, tunnel)
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close(t.Context())

	//nolint:gosec
	schemaName := fmt.Sprintf("test_%d", rand.Uint64())
	_, err = conn.Exec(t.Context(), "CREATE SCHEMA "+schemaName)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}
	defer func() {
		_, _ = conn.Exec(t.Context(), fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schemaName))
	}()

	parentTable := schemaName + ".empty_partitioned"
	_, err = conn.Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL,
			partition_key INT NOT NULL,
			value TEXT,
			PRIMARY KEY (partition_key, id)
		) PARTITION BY RANGE (partition_key)
	`, parentTable))
	require.NoError(t, err)

	for i := range 4 {
		child := fmt.Sprintf("%s.empty_child_%d", schemaName, i)
		_, err = conn.Exec(t.Context(), fmt.Sprintf(
			`CREATE TABLE %s PARTITION OF %s FOR VALUES FROM (%d) TO (%d)`,
			child, parentTable, i*25, (i+1)*25))
		require.NoError(t, err)
	}

	c := &PostgresConnector{
		connStr: connStr,
		Config:  &protos.PostgresConfig{},
		conn:    conn,
		logger:  log.NewStructuredLogger(slog.With(slog.String(string(shared.FlowNameKey), "testEmptyPartitioned"))),
	}
	query := fmt.Sprintf(`SELECT * FROM %s WHERE ctid BETWEEN {{.start}} AND {{.end}}`, parentTable)
	partitions, err := c.GetQRepPartitions(t.Context(), &protos.QRepConfig{
		FlowJobName:           "test_ctid_empty_partitioned",
		NumRowsPerPartition:   10,
		NumPartitionsOverride: 4,
		Query:                 query,
		WatermarkTable:        parentTable,
		WatermarkColumn:       "ctid",
		Env:                   map[string]string{"PEERDB_POSTGRES_APPLY_CTID_BLOCK_PARTITIONING_OVERRIDE": "true"},
	}, nil)
	require.NoError(t, err)
	require.Empty(t, partitions, "empty partitioned table should produce no partitions")
}

func TestCTIDPartitioningGroupingWhenChildrenExceedBudget(t *testing.T) {
	t.Parallel()
	connStr := internal.GetCatalogConnectionStringFromEnv(t.Context())

	config, err := pgx.ParseConfig(connStr)
	if err != nil {
		t.Fatalf("Failed to parse config: %v", err)
	}

	tunnel, err := utils.NewSSHTunnel(t.Context(), nil)
	if err != nil {
		t.Fatalf("Failed to create tunnel: %v", err)
	}
	defer tunnel.Close()

	conn, err := NewPostgresConnFromConfig(t.Context(), config, "", nil, tunnel)
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close(t.Context())

	//nolint:gosec
	schemaName := fmt.Sprintf("test_%d", rand.Uint64())
	_, err = conn.Exec(t.Context(), "CREATE SCHEMA "+schemaName)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}
	defer func() {
		_, _ = conn.Exec(t.Context(), fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schemaName))
	}()

	parentTable := schemaName + ".many_children"
	_, err = conn.Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL,
			partition_key INT NOT NULL,
			value TEXT,
			PRIMARY KEY (partition_key, id)
		) PARTITION BY RANGE (partition_key)
	`, parentTable))
	require.NoError(t, err)

	numChildren := 20
	numPartitionsBudget := uint32(8)
	allChildren := make([]string, numChildren)
	for i := range numChildren {
		allChildren[i] = fmt.Sprintf("%s.many_child_%d", schemaName, i)
		lo := i * 5
		hi := (i + 1) * 5
		_, err = conn.Exec(t.Context(), fmt.Sprintf(
			`CREATE TABLE %s PARTITION OF %s FOR VALUES FROM (%d) TO (%d)`,
			allChildren[i], parentTable, lo, hi))
		require.NoError(t, err)
	}

	for i := range numChildren {
		for j := range 10 {
			_, err = conn.Exec(t.Context(), fmt.Sprintf(
				`INSERT INTO %s (partition_key, value) VALUES ($1, $2)`, parentTable),
				i*5+(j%5), fmt.Sprintf("val_%d_%d", i, j))
			require.NoError(t, err)
		}
	}

	c := &PostgresConnector{
		connStr: connStr,
		Config:  &protos.PostgresConfig{},
		conn:    conn,
		logger:  log.NewStructuredLogger(slog.With(slog.String(string(shared.FlowNameKey), "testGrouping"))),
	}
	query := fmt.Sprintf(`SELECT * FROM %s WHERE ctid BETWEEN {{.start}} AND {{.end}}`, parentTable)
	partitions, err := c.GetQRepPartitions(t.Context(), &protos.QRepConfig{
		FlowJobName:           "test_ctid_grouping",
		NumRowsPerPartition:   10,
		NumPartitionsOverride: numPartitionsBudget,
		Query:                 query,
		WatermarkTable:        parentTable,
		WatermarkColumn:       "ctid",
		Env:                   map[string]string{"PEERDB_POSTGRES_APPLY_CTID_BLOCK_PARTITIONING_OVERRIDE": "true"},
	}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, partitions)
	require.LessOrEqual(t, len(partitions), int(numPartitionsBudget),
		"total partitions should not exceed budget")

	// Verify all children are covered
	coveredChildren := make(map[string]bool)
	for _, p := range partitions {
		require.NotEmpty(t, p.ChildTableRanges)
		for _, ctr := range p.ChildTableRanges {
			require.NotNil(t, ctr.Range)
			require.NotEmpty(t, ctr.Table)
			coveredChildren[ctr.Table] = true
		}
	}
	require.Len(t, coveredChildren, numChildren,
		"all %d non-empty children should be covered", numChildren)
	for _, child := range allChildren {
		require.True(t, coveredChildren[child], "child %s should be covered", child)
	}
}

func TestCtidPartitionsForPartitionedTableOffsetNumberBounds(t *testing.T) {
	t.Parallel()
	pp := PartitionParams{
		numPartitions: 3,
		logger:        log.NewStructuredLogger(slog.With(slog.String(string(shared.FlowNameKey), "testOffsetBounds"))),
	}
	leafBlocks := map[string]int64{
		"public.t1": 100,
		"public.t2": 50,
		"public.t3": 10,
	}
	partitions, err := ctidPartitionsForPartitionedTable(pp, leafBlocks, 160)
	require.NoError(t, err)
	require.NotEmpty(t, partitions)

	for _, p := range partitions {
		require.NotEmpty(t, p.ChildTableRanges)
		for _, ctr := range p.ChildTableRanges {
			require.LessOrEqual(t, ctr.Range.Start.OffsetNumber, uint32(math.MaxUint16),
				"start OffsetNumber must fit in uint16")
			require.LessOrEqual(t, ctr.Range.End.OffsetNumber, uint32(math.MaxUint16),
				"end OffsetNumber must fit in uint16")
		}
	}

	// Verify all tables are covered and block ranges are contiguous within each table
	tableRanges := make(map[string][][2]uint32) // table -> list of [startBlock, endBlock]
	for _, p := range partitions {
		for _, ctr := range p.ChildTableRanges {
			tableRanges[ctr.Table] = append(tableRanges[ctr.Table],
				[2]uint32{ctr.Range.Start.BlockNumber, ctr.Range.End.BlockNumber})
		}
	}
	require.Len(t, tableRanges, 3)
	// First range of each table should start at block 0
	for _, ranges := range tableRanges {
		require.Equal(t, uint32(0), ranges[0][0], "first range should start at block 0")
	}
}

// returns the number of rows inserted
func prepareTestData(t *testing.T, pool *pgx.Conn, schema string, includeNulls bool) int {
	t.Helper()

	// Define the start and end times
	startTime := time.Date(2010, time.January, 1, 10, 0, 0, 0, time.UTC)
	endTime := time.Date(2010, time.January, 31, 10, 0, 0, 0, time.UTC)

	rowsCount := 0
	for tm := startTime; tm.Before(endTime); tm = tm.Add(24 * time.Hour) {
		rowsCount += 1
		_, err := pool.Exec(t.Context(), fmt.Sprintf(`
			INSERT INTO %s.test (value, "from") VALUES ($1, $2)
		`, schema), rowsCount, tm)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	if includeNulls {
		// add some rows with null "from" value to ensure they get partitioned correctly as well
		for i := range 12 {
			rowsCount += 1
			_, err := pool.Exec(t.Context(), fmt.Sprintf(`
				INSERT INTO %s.test (value, "from") VALUES ($1, NULL)
			`, schema), rowsCount+i+1)
			if err != nil {
				t.Fatalf("Failed to insert test data with null from value: %v", err)
			}
		}
	}

	return rowsCount
}
