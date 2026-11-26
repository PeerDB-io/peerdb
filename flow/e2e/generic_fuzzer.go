package e2e

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

// fuzzSeed seeds every GenericFuzzer test, so runs are reproducible. Change it to explore other inputs.
const fuzzSeed uint64 = 1987

// fuzzSeedNames are always tried, before the randomly generated names.
var fuzzSeedNames = []string{
	// Avro primitive type names, which the spec reserves and so cannot name a record
	"null", "boolean", "int", "long", "float", "double", "bytes", "string",
	// Avro complex type keywords
	"record", "enum", "array", "map", "fixed",
	// Go basic types not listed above
	"bool", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64", "uintptr",
	"byte", "rune", "float32", "float64", "complex64", "complex128",
	// shapes ConvertToAvroCompatibleName has to rewrite or prefix
	"_", "_leading_underscore", "1leading_digit", "123",
}

// fuzzNameAlphabet is the rune set random names are drawn from.
// It stays within what ClickHouse accepts as quoted identifiers.
const fuzzNameAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789_"

// FuzzColumn is one column of GenericFuzzer.Test_Fuzz_Values.
type FuzzColumn struct {
	// Generate returns a SQL literal for the source dialect, "NULL" included.
	Generate func(r *rand.Rand) string
	// SourceType is the column type as written in the source CREATE TABLE, nullability included, e.g. "INT4 NOT NULL".
	SourceType string
}

type GenericFuzzerConfig struct {
	// Columns maps a column name to its source type and value generator, used by Test_Fuzz_Values.
	Columns map[string]FuzzColumn
	// RandomNames is how many generated names are added to the seed names. Defaults to 16.
	RandomNames int
	// Rows is how many rows are inserted before the initial snapshot, and again during CDC. Defaults to 50.
	Rows int
}

// GenericFuzzer checks that tables propagate to the destination with generated
// table names, column names and values, through both the initial snapshot and CDC.
type GenericFuzzer struct {
	GenericSuite
	cfg GenericFuzzerConfig
}

func SetupGenericFuzzer[T GenericSuite](f func(t *testing.T) T, cfg GenericFuzzerConfig) func(t *testing.T) GenericFuzzer {
	if cfg.RandomNames == 0 {
		cfg.RandomNames = 16
	}
	if cfg.Rows == 0 {
		cfg.Rows = 50
	}
	return func(t *testing.T) GenericFuzzer {
		t.Helper()
		return GenericFuzzer{GenericSuite: f(t), cfg: cfg}
	}
}

// Test_Fuzz_Table_Names mirrors one table per name into a destination table of that name.
// Source tables keep fixed names so a failure points at the destination side.
// Goal: Probe the resilience to unusual table names
func (s GenericFuzzer) Test_Fuzz_Table_Names() {
	t := s.T()
	names := s.names(s.rand())

	pairs := make([]string, 0, 2*len(names))
	for i, name := range names {
		srcTable := fmt.Sprintf("fz_tbl_%d", i)
		require.NoError(t, s.Source().Exec(t.Context(), fmt.Sprintf(
			`CREATE TABLE %s (id SERIAL PRIMARY KEY, val TEXT NOT NULL)`, AttachSchema(s, srcTable))))
		pairs = append(pairs, srcTable, name)
	}

	s.mirrorAndCompare("fz_tables", pairs, "id,val", func(phase string) error {
		for i := range names {
			values := make([]string, s.cfg.Rows)
			for row := range values {
				values[row] = fmt.Sprintf("('%s_%d')", phase, row)
			}
			if err := s.Source().Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s (val) VALUES %s`,
				AttachSchema(s, fmt.Sprintf("fz_tbl_%d", i)), strings.Join(values, ","))); err != nil {
				return err
			}
		}
		return nil
	})
}

// Test_Fuzz_Column_Names mirrors one table with multiple TEXT columns. One per name generated name and alternating NOT NULL and NULLABLE.
// Goal: Probe the resilience to unusual or edge-case column names in the destination.
func (s GenericFuzzer) Test_Fuzz_Column_Names() {
	t := s.T()
	r := s.rand()
	names := s.names(r)

	defs := make([]string, len(names))
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = s.quote(name)
		defs[i] = quoted[i] + " TEXT"
		if i%2 == 0 {
			defs[i] += " NOT NULL"
		}
	}
	const srcTable = "fz_columns"
	require.NoError(t, s.Source().Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id SERIAL PRIMARY KEY, %s)`, AttachSchema(s, srcTable), strings.Join(defs, ", "))))

	s.mirrorAndCompare("fz_columns", []string{srcTable, srcTable}, "id,"+strings.Join(quoted, ","), func(phase string) error {
		return s.insertRows(srcTable, quoted, func(row int) []string {
			values := make([]string, len(names))
			for i := range names {
				if i%2 == 1 && r.IntN(4) == 0 {
					values[i] = "NULL"
				} else {
					values[i] = fmt.Sprintf("'%s_%d_%d'", phase, row, i)
				}
			}
			return values
		})
	})
}

// Test_Fuzz_Values mirrors one table with the configured columns, filled by their generators.
func (s GenericFuzzer) Test_Fuzz_Values() {
	t := s.T()
	if len(s.cfg.Columns) == 0 {
		t.Skip("no fuzz columns configured")
	}
	r := s.rand()

	names := make([]string, 0, len(s.cfg.Columns))
	for name := range s.cfg.Columns {
		names = append(names, name)
	}
	slices.Sort(names) // map order is random, keep runs reproducible from the seed

	defs := make([]string, len(names))
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = s.quote(name)
		defs[i] = quoted[i] + " " + s.cfg.Columns[name].SourceType
	}
	const srcTable = "fz_values"
	require.NoError(t, s.Source().Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id SERIAL PRIMARY KEY, %s)`, AttachSchema(s, srcTable), strings.Join(defs, ", "))))

	s.mirrorAndCompare("fz_values", []string{srcTable, srcTable}, "id,"+strings.Join(quoted, ","), func(string) error {
		return s.insertRows(srcTable, quoted, func(int) []string {
			values := make([]string, len(names))
			for i, name := range names {
				values[i] = s.cfg.Columns[name].Generate(r)
			}
			return values
		})
	})
}

// mirrorAndCompare runs a mirror with initial snapshot over the source/destination table pairs.
// insert is called once before the mirror starts, so those rows go through the snapshot,
// and once after, so those go through CDC. Every pair must then match on cols.
func (s GenericFuzzer) mirrorAndCompare(job string, pairs []string, cols string, insert func(phase string) error) {
	t := s.T()
	require.NoError(t, insert("snapshot"))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, job),
		TableMappings: TableMappings(s, pairs...),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	// PEERDB_NULLABLE added to use real null values rather than go type defaults.
	// It also routes nullable columns through Avro ["null", T] unions.
	flowConnConfig.Env = map[string]string{"PEERDB_NULLABLE": "true"}

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvNoError(t, env, insert("cdc"))

	for pair := range slices.Chunk(pairs, 2) {
		EnvWaitForEqualTablesWithNames(env, s, fmt.Sprintf("table %q to %q", pair[0], pair[1]), pair[0], pair[1], cols)
	}
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// insertRows inserts cfg.Rows rows into table in a single statement, with values(row) as each row's literals.
func (s GenericFuzzer) insertRows(table string, quotedCols []string, values func(row int) []string) error {
	rows := make([]string, s.cfg.Rows)
	for row := range rows {
		rows[row] = "(" + strings.Join(values(row), ",") + ")"
	}
	return s.Source().Exec(s.T().Context(), fmt.Sprintf(`INSERT INTO %s (%s) VALUES %s`,
		AttachSchema(s, table), strings.Join(quotedCols, ","), strings.Join(rows, ",")))
}

// rand returns a generator seeded with fuzzSeed.
func (s GenericFuzzer) rand() *rand.Rand {
	return rand.New(rand.NewPCG(fuzzSeed, fuzzSeed)) //nolint:gosec // G404: fuzzing needs a seedable, reproducible generator
}

// names returns the seed names followed by cfg.RandomNames unique random names.
// It leaves out "id", used as primary key, and PeerDB's own _peerdb columns.
func (s GenericFuzzer) names(r *rand.Rand) []string {
	names := slices.Clone(fuzzSeedNames)
	for len(names) < len(fuzzSeedNames)+s.cfg.RandomNames {
		b := make([]byte, 1+r.IntN(30))
		for i := range b {
			b[i] = fuzzNameAlphabet[r.IntN(len(fuzzNameAlphabet))]
		}
		name := string(b)
		if name != "id" && !strings.HasPrefix(name, "_peerdb") && !slices.Contains(names, name) {
			names = append(names, name)
		}
	}
	return names
}

// quote quotes an identifier for the source. ClickHouse accepts both quote styles,
// so the result also works in the destination query.
func (s GenericFuzzer) quote(name string) string {
	if _, isMySQL := s.Source().(*MySqlSource); isMySQL {
		return "`" + name + "`"
	}
	return common.QuoteIdentifier(name)
}
