//go:build tilt

package clickhouse

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/pkg/testutil"
)

// TestValidatorsAgreeWithClickHouseParser searches for disagreements between our validators and ClickHouse's parser.
// These are parser checks; column references and expression result types are not evaluated.
func TestValidatorsAgreeWithClickHouseParser(t *testing.T) {
	addr := fmt.Sprintf("%s:%d", testutil.ClickHouseTestHost(), testutil.ClickHouseTestPort())
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})
	require.NoError(t, conn.Ping(t.Context()))

	// formatted returns canonical SQL, or nil when ClickHouse rejects the query.
	// formatQuerySingleLineOrNull parses without running anything, and takes the fragment as a query parameter,
	// so fragments can't affect this check itself.
	formatted := func(t *testing.T, queryExpr string, fragment string) *string {
		t.Helper()
		ctx := clickhouse.Context(t.Context(), clickhouse.WithParameters(clickhouse.Parameters{"fragment": fragment}))
		var result *string
		require.NoError(t, conn.QueryRow(ctx, "SELECT formatQuerySingleLineOrNull("+queryExpr+")").Scan(&result))
		return result
	}

	t.Run("column types", func(t *testing.T) {
		for _, columnType := range append(acceptedRandomFragments(fragmentRand(5), ValidateColumnType, 500), validColumnTypes...) {
			require.NoError(t, ValidateColumnType(columnType), columnType)
			ddl := formatted(t, "concat('CREATE TABLE t (x ', {fragment:String}, ', y UInt8) ENGINE = Null')", columnType)
			if ddl == nil {
				continue // Rejected SQL cannot change the statement.
			}
			// ClickHouse rewrites what it parses, so compare against the type as it parses on its own.
			cast := formatted(t, "concat('SELECT CAST(1 AS ', {fragment:String}, ')')", columnType)
			require.NotNil(t, cast, "column type %q parses in CREATE TABLE but not in CAST", columnType)
			canonicalType, ok := strings.CutPrefix(*cast, "SELECT CAST(1, '")
			require.True(t, ok, *cast)
			canonicalType, ok = strings.CutSuffix(canonicalType, "')")
			require.True(t, ok, *cast)
			require.True(t, strings.HasPrefix(*ddl, "CREATE TABLE t (`x` "+canonicalType+", `y` UInt8) ENGINE = "),
				"column type %q changed the statement: %s", columnType, *ddl)
		}
	})

	t.Run("expressions", func(t *testing.T) {
		for _, expression := range append(acceptedRandomFragments(fragmentRand(7), ValidateExpression, 1000), validExpressions...) {
			require.NoError(t, ValidateExpression(expression), expression)
			ddl := formatted(t, "concat('CREATE TABLE t (id Int64) ENGINE = MergeTree ORDER BY id PARTITION BY (', "+
				"{fragment:String}, ') SETTINGS index_granularity = 8192')", expression)
			if ddl == nil {
				continue // Rejected SQL cannot change the statement.
			}
			// ClickHouse rewrites what it parses, so compare against the expression as it parses on its own.
			selectQuery := formatted(t, "concat('SELECT (', {fragment:String}, ')')", expression)
			require.NotNil(t, selectQuery, "expression %q did not parse in SELECT", expression)
			canonicalExpression, ok := strings.CutPrefix(*selectQuery, "SELECT ")
			require.True(t, ok, *selectQuery)
			require.Equal(t,
				"CREATE TABLE t (`id` Int64) ENGINE = MergeTree PARTITION BY "+canonicalExpression+
					" ORDER BY id SETTINGS index_granularity = 8192",
				*ddl, "expression %q changed the statement", expression)
		}
	})

	t.Run("sharding keys", func(t *testing.T) {
		shardingKeys := append(acceptedRandomFragments(fragmentRand(9), ValidateExpression, 500),
			"id", "cityHash64(id)", "rand()", "cityHash64(id) % 4", "id, other_id")
		for _, expression := range shardingKeys {
			require.NoError(t, ValidateExpression(expression), expression)
			// A following policy argument catches fragments that change the engine's argument boundaries.
			ddl := formatted(t, fmt.Sprintf("concat(%s, {fragment:String}, %s)",
				QuoteLiteral("CREATE TABLE t (id Int64) ENGINE = Distributed('cluster', 'db', 'local', ("),
				QuoteLiteral("), 'policy')")), expression)
			if ddl == nil {
				continue // Rejected SQL cannot change the statement.
			}
			selectQuery := formatted(t, "concat('SELECT (', {fragment:String}, ')')", expression)
			require.NotNil(t, selectQuery, "expression %q did not parse in SELECT", expression)
			canonicalExpression, ok := strings.CutPrefix(*selectQuery, "SELECT ")
			require.True(t, ok, *selectQuery)
			require.Equal(t,
				"CREATE TABLE t (`id` Int64) ENGINE = Distributed('cluster', 'db', 'local', "+canonicalExpression+", 'policy')",
				*ddl, "sharding key %q changed the engine arguments", expression)
		}
	})
}

func acceptedRandomFragments(r *rand.Rand, validate func(string) error, count int) []string {
	seen := make(map[string]struct{}, count)
	accepted := make([]string, 0, count)
	for range 200 * count {
		if len(accepted) == count {
			break
		}
		fragment := randomFragment(r)
		if _, ok := seen[fragment]; ok || validate(fragment) != nil {
			continue
		}
		seen[fragment] = struct{}{}
		accepted = append(accepted, fragment)
	}
	return accepted
}
