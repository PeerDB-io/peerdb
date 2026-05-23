package connpostgres

import (
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
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
			// The test table fits in a single 8 KB page, so block-based CTID partitioning only emit 1 partition.
			// Override with PEERDB_POSTGRES_APPLY_CTID_BLOCK_PARTITIONING_OVERRIDE = false to test legacy behavior
			// with NTileBucketPartitioning. We have separate tests below for testing CtidBlockPartitioning logic.
			Env: map[string]string{"PEERDB_POSTGRES_APPLY_CTID_BLOCK_PARTITIONING_OVERRIDE": "false"},
		},
		expectedNumPartitions: expectedNum,
	}
}

func setupTestSchema(t *testing.T) (string, *pgx.Conn, string) {
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

	schemaName := "test_" + strings.ToLower(common.RandomString(8))

	_, err = conn.Exec(t.Context(), fmt.Sprintf(`CREATE SCHEMA %s;`, schemaName))
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}
	t.Cleanup(func() {
		if _, err := conn.Exec(t.Context(), fmt.Sprintf(`DROP SCHEMA %s CASCADE;`, schemaName)); err != nil {
			t.Logf("Failed to drop schema: %v", err)
		}
	})

	return schemaName, conn, catalogConnStr
}

func setupTestSchemaAndTable(t *testing.T) (string, *pgx.Conn) {
	t.Helper()
	schemaName, conn, _ := setupTestSchema(t)

	_, err := conn.Exec(t.Context(), fmt.Sprintf(`
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
	schemaName, conn := setupTestSchemaAndTable(t)

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
	schemaName, conn := setupTestSchemaAndTable(t)

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
	schemaName, conn, connStr := setupTestSchema(t)

	parentTable := schemaName + ".partitioned_test"
	_, err := conn.Exec(t.Context(), fmt.Sprintf(`
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

	rowsPerPartition := 25
	numChildTables := 4
	childTables := make([]string, numChildTables)

	for i := range numChildTables {
		childTables[i] = fmt.Sprintf("%s.child_%d", schemaName, i)
		lo := i * rowsPerPartition
		hi := (i + 1) * rowsPerPartition
		_, err = conn.Exec(t.Context(), fmt.Sprintf(
			`CREATE TABLE %s PARTITION OF %s FOR VALUES FROM (%d) TO (%d)`,
			childTables[i], parentTable, lo, hi))
		if err != nil {
			t.Fatalf("Failed to create child partition %d: %v", i, err)
		}
	}

	for i := range numChildTables {
		for j := range rowsPerPartition {
			_, err = conn.Exec(t.Context(), fmt.Sprintf(
				`INSERT INTO %s (partition_key, value) VALUES ($1, $2)`, parentTable),
				i*rowsPerPartition+j, fmt.Sprintf("val_%d_%d", i, j))
			if err != nil {
				t.Fatalf("Failed to insert row: %v", err)
			}
		}
	}

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
	}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, partitions)

	childTableCounts := make(map[string]int)
	for _, p := range partitions {
		require.Nil(t, p.Range)
		require.NotEmpty(t, p.ChildTableRanges)
		for _, ctr := range p.ChildTableRanges {
			require.NotEmpty(t, ctr.Table)
			childTableCounts[ctr.Table]++
		}
	}

	require.Len(t, childTableCounts, numChildTables)
	for _, child := range childTables {
		require.Positive(t, childTableCounts[child], "child %s should appear in at least one partition", child)
	}
}

func TestCTIDPartitioningOnMultiLevelPartitionedTable(t *testing.T) {
	t.Parallel()
	_, conn, connStr := setupTestSchema(t)

	// Use a mixed-case schema name as well to verify OID-based lookups work
	// for both schema and table identifiers (to_regclass lowercases unquoted identifiers).
	schemaName := "Test_" + common.RandomString(8)
	schemaNameDDL := `"` + schemaName + `"`
	_, err := conn.Exec(t.Context(), `CREATE SCHEMA `+schemaNameDDL)
	require.NoError(t, err)
	t.Cleanup(func() {
		if _, err := conn.Exec(t.Context(), `DROP SCHEMA `+schemaNameDDL+` CASCADE`); err != nil {
			t.Logf("Failed to drop schema: %v", err)
		}
	})

	// Mixed-case names to verify OID-based lookups work (to_regclass lowercases unquoted identifiers).
	// rootTable is the unquoted form passed to PeerDB; rootTableDDL is the quoted form for SQL DDL.
	rootTable := schemaName + ".MultiLevel"
	rootTableDDL := schemaNameDDL + `."MultiLevel"`
	_, err = conn.Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL,
			region INT NOT NULL,
			category INT NOT NULL,
			value TEXT,
			PRIMARY KEY (region, category, id)
		) PARTITION BY RANGE (region)
	`, rootTableDDL))
	require.NoError(t, err)

	rowsPerPartition := 20
	numMidLevelTables := 2
	numLeafChildTablesPerMidLevelTable := 3

	// Two mid-level partitions, each sub-partitioned by category
	for r := range numMidLevelTables {
		mid := fmt.Sprintf(`%s."Region_%d"`, schemaNameDDL, r)
		_, err = conn.Exec(t.Context(), fmt.Sprintf(
			`CREATE TABLE %s PARTITION OF %s FOR VALUES FROM (%d) TO (%d) PARTITION BY RANGE (category)`,
			mid, rootTableDDL, r*50, (r+1)*50))
		require.NoError(t, err)

		// Three leaf partitions per mid-level
		for c := range numLeafChildTablesPerMidLevelTable {
			leaf := fmt.Sprintf(`%s."Region_%d_Cat_%d"`, schemaNameDDL, r, c)
			_, err = conn.Exec(t.Context(), fmt.Sprintf(
				`CREATE TABLE %s PARTITION OF %s FOR VALUES FROM (%d) TO (%d)`,
				leaf, mid, c*33, (c+1)*33))
			require.NoError(t, err)
		}
	}

	// 6 leaf tables total, insert 20 rows into each
	// expectedLeaves uses unquoted names since that's what format('%s.%s', ...) in pg_class returns
	expectedLeaves := make([]string, 0, numLeafChildTablesPerMidLevelTable*numMidLevelTables)
	for r := range numMidLevelTables {
		for c := range numLeafChildTablesPerMidLevelTable {
			leaf := fmt.Sprintf("%s.Region_%d_Cat_%d", schemaName, r, c)
			expectedLeaves = append(expectedLeaves, leaf)
			for j := range rowsPerPartition {
				_, err = conn.Exec(t.Context(), fmt.Sprintf(
					`INSERT INTO %s (region, category, value) VALUES ($1, $2, $3)`, rootTableDDL),
					r*50, c*33+j%33, fmt.Sprintf("v_%d_%d_%d", r, c, j))
				require.NoError(t, err)
			}
		}
	}

	c := &PostgresConnector{
		connStr: connStr,
		Config:  &protos.PostgresConfig{},
		conn:    conn,
		logger:  log.NewStructuredLogger(slog.With(slog.String(string(shared.FlowNameKey), "testMultiLevel"))),
	}
	query := fmt.Sprintf(`SELECT * FROM %s WHERE ctid BETWEEN {{.start}} AND {{.end}}`, rootTableDDL)
	partitions, err := c.GetQRepPartitions(t.Context(), &protos.QRepConfig{
		FlowJobName:           "test_ctid_multi_level",
		NumRowsPerPartition:   10,
		NumPartitionsOverride: 12,
		Query:                 query,
		WatermarkTable:        rootTable,
		WatermarkColumn:       "ctid",
	}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, partitions)

	childTableCounts := make(map[string]int)
	for _, p := range partitions {
		require.Nil(t, p.Range)
		require.NotEmpty(t, p.ChildTableRanges)
		for _, ctr := range p.ChildTableRanges {
			require.NotEmpty(t, ctr.Table)
			childTableCounts[ctr.Table]++
		}
	}

	require.Len(t, childTableCounts, numLeafChildTablesPerMidLevelTable*numMidLevelTables)
	for _, leaf := range expectedLeaves {
		require.Positive(t, childTableCounts[leaf])
	}
	for r := range numMidLevelTables {
		mid := fmt.Sprintf("%s.Region_%d", schemaName, r)
		require.Zero(t, childTableCounts[mid], mid)
	}
}

func TestCTIDPartitioningOnEmptyPartitionedTable(t *testing.T) {
	t.Parallel()
	schemaName, conn, connStr := setupTestSchema(t)

	parentTable := schemaName + ".empty_partitioned"
	_, err := conn.Exec(t.Context(), fmt.Sprintf(`
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
	}, nil)
	require.NoError(t, err)
	require.Empty(t, partitions)
}

func TestCTIDPartitioningGroupingWhenChildrenExceedBudget(t *testing.T) {
	t.Parallel()
	schemaName, conn, connStr := setupTestSchema(t)

	parentTable := schemaName + ".many_children"
	_, err := conn.Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL,
			partition_key INT NOT NULL,
			value TEXT,
			PRIMARY KEY (partition_key, id)
		) PARTITION BY RANGE (partition_key)
	`, parentTable))
	require.NoError(t, err)

	numRowsPerChildTable := 10
	numChildTables := 20
	numPartitionsBudget := uint32(8)
	allChildren := make([]string, numChildTables)
	for i := range numChildTables {
		allChildren[i] = fmt.Sprintf("%s.many_child_%d", schemaName, i)
		lo := i * 5
		hi := (i + 1) * 5
		_, err = conn.Exec(t.Context(), fmt.Sprintf(
			`CREATE TABLE %s PARTITION OF %s FOR VALUES FROM (%d) TO (%d)`,
			allChildren[i], parentTable, lo, hi))
		require.NoError(t, err)
	}

	for i := range numChildTables {
		for j := range numRowsPerChildTable {
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
	}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, partitions)
	require.LessOrEqual(t, len(partitions), int(numPartitionsBudget))

	childTablesCovered := make(map[string]bool)
	for _, p := range partitions {
		require.NotEmpty(t, p.ChildTableRanges)
		for _, ctr := range p.ChildTableRanges {
			require.NotEmpty(t, ctr.Table)
			childTablesCovered[ctr.Table] = true
		}
	}
	require.Len(t, childTablesCovered, numChildTables)
	for _, child := range allChildren {
		require.True(t, childTablesCovered[child])
	}
}

func TestCtidPartitionsForChildTablesOffsetNumberBounds(t *testing.T) {
	t.Parallel()
	pp := PartitionParams{
		numPartitions: 8,
		logger:        log.NewStructuredLogger(slog.With(slog.String(string(shared.FlowNameKey), "testOffsetBounds"))),
	}
	leafBlocks := map[string]int64{
		"public.t1": 100,
		"public.t2": 45,
		"public.t3": 10,
	}
	// blocksPerPartition = DivCeil(155, 8) = 20
	partitions, err := ctidPartitionsForChildTables(pp, leafBlocks)
	require.NoError(t, err)
	require.Len(t, partitions, 8)

	childRange := func(table string, startBlock, endBlock uint32) *protos.ChildTableRange {
		return &protos.ChildTableRange{
			Table: table,
			Start: startBlock,
			End:   endBlock,
		}
	}
	expected := [][]*protos.ChildTableRange{
		{childRange("public.t1", 0, 19)},
		{childRange("public.t1", 20, 39)},
		{childRange("public.t1", 40, 59)},
		{childRange("public.t1", 60, 79)},
		{childRange("public.t1", 80, 99)},
		{childRange("public.t2", 0, 19)},
		{childRange("public.t2", 20, 39)},
		{childRange("public.t2", 40, 44), childRange("public.t3", 0, 9)},
	}

	for i, p := range partitions {
		require.Len(t, p.ChildTableRanges, len(expected[i]))
		for j, ctr := range p.ChildTableRanges {
			exp := expected[i][j]
			msg := fmt.Sprintf("partition %d range %d", i, j)
			assert.Equal(t, exp.Table, ctr.Table, msg)
			assert.Equal(t, exp.Start, ctr.Start, msg)
			assert.Equal(t, exp.End, ctr.End, msg)
		}
	}
}

func TestCTIDPartitioningOnInheritedTable(t *testing.T) {
	t.Parallel()
	_, conn, connStr := setupTestSchema(t)

	// Use a mixed-case schema name as well to verify OID-based lookups work
	// for both schema and table identifiers (to_regclass lowercases unquoted identifiers).
	schemaName := "Test_" + common.RandomString(8)
	schemaNameDDL := `"` + schemaName + `"`
	_, err := conn.Exec(t.Context(), `CREATE SCHEMA `+schemaNameDDL)
	require.NoError(t, err)
	t.Cleanup(func() {
		if _, err := conn.Exec(t.Context(), `DROP SCHEMA `+schemaNameDDL+` CASCADE`); err != nil {
			t.Logf("Failed to drop schema: %v", err)
		}
	})

	// Mixed-case names to verify OID-based lookups work (to_regclass lowercases unquoted identifiers).
	// parentTable is the unquoted form passed to PeerDB; parentTableDDL is the quoted form for SQL DDL.
	parentTable := schemaName + ".ParentInh"
	parentTableDDL := schemaNameDDL + `."ParentInh"`
	_, err = conn.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id SERIAL PRIMARY KEY, value TEXT)`, parentTableDDL))
	require.NoError(t, err)

	numChildren := 3
	rowsPerTable := 20
	childTablesDDL := make([]string, numChildren)
	for i := range numChildren {
		childTablesDDL[i] = fmt.Sprintf(`%s."ChildInh_%d"`, schemaNameDDL, i)
		_, err = conn.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s () INHERITS (%s)`, childTablesDDL[i], parentTableDDL))
		require.NoError(t, err)
	}

	for j := range rowsPerTable {
		_, err = conn.Exec(t.Context(), fmt.Sprintf(
			`INSERT INTO %s (value) VALUES ($1)`, parentTableDDL), fmt.Sprintf("parent_%d", j))
		require.NoError(t, err)
	}
	for i, child := range childTablesDDL {
		for j := range rowsPerTable {
			_, err = conn.Exec(t.Context(), fmt.Sprintf(
				`INSERT INTO %s (value) VALUES ($1)`, child), fmt.Sprintf("child_%d_%d", i, j))
			require.NoError(t, err)
		}
	}

	c := &PostgresConnector{
		connStr: connStr,
		Config:  &protos.PostgresConfig{},
		conn:    conn,
		logger:  log.NewStructuredLogger(slog.With(slog.String(string(shared.FlowNameKey), "testInherited"))),
	}
	partitions, err := c.GetQRepPartitions(t.Context(), &protos.QRepConfig{
		FlowJobName:           "test_ctid_inherited",
		NumRowsPerPartition:   10,
		NumPartitionsOverride: 8,
		WatermarkTable:        parentTable,
		WatermarkColumn:       "ctid",
	}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, partitions)

	childTablesCovered := make(map[string]bool)
	for _, p := range partitions {
		require.NotEmpty(t, p.ChildTableRanges)
		for _, ctr := range p.ChildTableRanges {
			childTablesCovered[ctr.Table] = true
		}
	}
	// pg_class returns unquoted names from format('%s.%s', ...) so assertions use the unquoted form
	require.Len(t, childTablesCovered, 1+numChildren)
	require.True(t, childTablesCovered[schemaName+".ParentInh"])
	for i := range numChildren {
		require.True(t, childTablesCovered[fmt.Sprintf("%s.ChildInh_%d", schemaName, i)])
	}
}

func TestCTIDPartitioningOnMultiLevelInheritedTable(t *testing.T) {
	t.Parallel()
	schemaName, conn, connStr := setupTestSchema(t)

	grandparent := schemaName + ".grandparent"
	_, err := conn.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id SERIAL PRIMARY KEY, value TEXT)`, grandparent))
	require.NoError(t, err)

	parent1 := schemaName + ".parent_1"
	parent2 := schemaName + ".parent_2"
	_, err = conn.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s () INHERITS (%s)`, parent1, grandparent))
	require.NoError(t, err)
	_, err = conn.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s () INHERITS (%s)`, parent2, grandparent))
	require.NoError(t, err)

	leaf1 := schemaName + ".leaf_1"
	leaf2 := schemaName + ".leaf_2"
	_, err = conn.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s () INHERITS (%s)`, leaf1, parent1))
	require.NoError(t, err)
	_, err = conn.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s () INHERITS (%s)`, leaf2, parent2))
	require.NoError(t, err)

	rowsPerTable := 15
	allTables := []string{grandparent, parent1, parent2, leaf1, leaf2}
	for _, tbl := range allTables {
		for j := range rowsPerTable {
			_, err = conn.Exec(t.Context(), fmt.Sprintf(
				`INSERT INTO %s (value) VALUES ($1)`, tbl), fmt.Sprintf("v_%d", j))
			require.NoError(t, err)
		}
	}

	c := &PostgresConnector{
		connStr: connStr,
		Config:  &protos.PostgresConfig{},
		conn:    conn,
		logger:  log.NewStructuredLogger(slog.With(slog.String(string(shared.FlowNameKey), "testMultiLevelInherited"))),
	}
	partitions, err := c.GetQRepPartitions(t.Context(), &protos.QRepConfig{
		FlowJobName:           "test_ctid_multi_inherited",
		NumRowsPerPartition:   10,
		NumPartitionsOverride: 10,
		WatermarkTable:        grandparent,
		WatermarkColumn:       "ctid",
	}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, partitions)

	tablesCovered := make(map[string]bool)
	for _, p := range partitions {
		require.NotEmpty(t, p.ChildTableRanges)
		for _, ctr := range p.ChildTableRanges {
			tablesCovered[ctr.Table] = true
		}
	}
	require.Len(t, tablesCovered, len(allTables))
	for _, tbl := range allTables {
		require.True(t, tablesCovered[tbl])
	}
}

func TestCTIDPartitioningOnEmptyInheritedTable(t *testing.T) {
	t.Parallel()
	schemaName, conn, connStr := setupTestSchema(t)

	parentTable := schemaName + ".empty_inh"
	_, err := conn.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id SERIAL PRIMARY KEY,value TEXT)`, parentTable))
	require.NoError(t, err)

	for i := range 3 {
		child := fmt.Sprintf("%s.empty_inh_child_%d", schemaName, i)
		_, err = conn.Exec(t.Context(), fmt.Sprintf(
			`CREATE TABLE %s () INHERITS (%s)`, child, parentTable))
		require.NoError(t, err)
	}

	c := &PostgresConnector{
		connStr: connStr,
		Config:  &protos.PostgresConfig{},
		conn:    conn,
		logger:  log.NewStructuredLogger(slog.With(slog.String(string(shared.FlowNameKey), "testAllEmptyInherited"))),
	}
	partitions, err := c.GetQRepPartitions(t.Context(), &protos.QRepConfig{
		FlowJobName:           "test_ctid_all_empty_inherited",
		NumRowsPerPartition:   10,
		NumPartitionsOverride: 4,
		WatermarkTable:        parentTable,
		WatermarkColumn:       "ctid",
	}, nil)
	require.NoError(t, err)
	require.Empty(t, partitions)
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
