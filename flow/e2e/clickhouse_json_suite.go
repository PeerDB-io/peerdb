package e2e

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// Tests for Postgres and MySQL JSON columns replicated to ClickHouse with PEERDB_CLICKHOUSE_ENABLE_JSON on
// and off, with and without PEERDB_NULLABLE. With the setting on, non-object documents are expected
// wrapped as {"_value": ...} from InternalVersion_WrapNonObjectJSON on.

func (s ClickHouseSuite) Test_JSON_CH() {
	emptyObject := "{}"
	s.testJSONColumns(jsonColumnMirrorConfig{
		tableName:          "test_json",
		nativeJSON:         true,
		expectedColumnType: "JSON",
		sourceNullLandsAs:  &emptyObject,
	})
}

func (s ClickHouseSuite) Test_JSON_CH_Nullable() {
	s.testJSONColumns(jsonColumnMirrorConfig{
		tableName:                  "test_json_nullable",
		nativeJSON:                 true,
		nullableDestinationColumns: true,
		expectedColumnType:         "Nullable(JSON)",
	})
}

func (s ClickHouseSuite) Test_JSON_CH_Disabled() {
	emptyString := ""
	s.testJSONColumns(jsonColumnMirrorConfig{
		tableName:          "test_json_off",
		expectedColumnType: "String",
		sourceNullLandsAs:  &emptyString,
	})
}

func (s ClickHouseSuite) Test_JSON_CH_Disabled_Nullable() {
	s.testJSONColumns(jsonColumnMirrorConfig{
		tableName:                  "test_json_off_nullable",
		nullableDestinationColumns: true,
		expectedColumnType:         "Nullable(String)",
	})
}

// Test_JSON_CH_OldVersion pins a native JSON mirror below InternalVersion_WrapNonObjectJSON and checks that
// object documents still land unchanged. Non-object documents are left out: at that version ClickHouse
// rejects them, which is what the wrapping fixes.
func (s ClickHouseSuite) Test_JSON_CH_OldVersion() {
	if mySource, ok := s.source.(*MySqlSource); ok && mySource.Config.Flavor == protos.MySqlFlavor_MYSQL_MARIA {
		s.t.Skip("skip maria, where JSON is stored as LONGTEXT and PeerDB does not map it to JSON yet")
	}

	srcTableName := "test_json_old"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := srcTableName + "_dst"
	docs := []string{`'{"key": "val", "number": 30, "bool": true}'`, `'{"nested": {"arr": [1, 2, null]}}'`, `'{}'`}
	insertDocs := func() {
		for _, doc := range docs {
			require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (json) VALUES (%s)`, srcFullName, doc)))
		}
	}

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, json JSON)`, srcFullName)))
	insertDocs()

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Version = shared.InternalVersion_WrapNonObjectJSON - 1
	flowConnConfig.Env = map[string]string{
		"PEERDB_CLICKHOUSE_ENABLE_JSON": "true",
		"PEERDB_FORCE_INTERNAL_VERSION": strconv.FormatUint(uint64(flowConnConfig.Version), 10),
	}

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	columnType, err := s.GetColumnType(dstTableName, "json")
	require.NoError(s.t, err)
	require.Equal(s.t, "JSON", columnType)

	rowsMatch := jsonBatchesMatch([]string{"json"}, nil, false)
	EnvWaitForEqualTablesWithNamesFunc(env, s, "compare initial load", srcTableName, dstTableName, "id,json", rowsMatch)
	insertDocs()
	EnvWaitForEqualTablesWithNamesFunc(env, s, "compare cdc", srcTableName, dstTableName, "id,json", rowsMatch)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

// Test_JSON_CH_ClassicReader runs the native JSON matrix with PEERDB_POSTGRES_FAST_PROCESS_JSON_COLUMNS off, so
// the Postgres reader takes the unmarshal/marshal path instead of the token walk.
func (s ClickHouseSuite) Test_JSON_CH_ClassicReader() {
	emptyObject := "{}"
	s.testJSONColumns(jsonColumnMirrorConfig{
		tableName:                     "test_json_classic",
		nativeJSON:                    true,
		disableFastProcessJSONColumns: true,
		expectedColumnType:            "JSON",
		sourceNullLandsAs:             &emptyObject,
	})
}

// Test_JSON_CH_AddColumnDefault adds JSON columns with defaults while the mirror runs. The row that
// predates the columns gets its value from the ClickHouse column default, which schema-change replay
// carries over from the Postgres default, wrapped like any other value.
func (s ClickHouseSuite) Test_JSON_CH_AddColumnDefault() {
	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("only applies to postgres")
	}

	srcTableName := "test_json_add_col_default"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := srcTableName + "_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY)", srcFullName)))
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf("INSERT INTO %s (id) VALUES (1)", srcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_CLICKHOUSE_ENABLE_JSON": "true"}

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id")

	for _, stmt := range []string{
		`ALTER TABLE %s ADD COLUMN c_scalar JSONB DEFAULT '"dark"'`,
		`ALTER TABLE %s ADD COLUMN c_array JSON DEFAULT '[1, 2]'`,
		`ALTER TABLE %s ADD COLUMN c_obj JSONB DEFAULT '{"a": 1}'`,
		`INSERT INTO %s (id, c_scalar, c_array, c_obj) VALUES (2, '"explicit"', '[3]', '{"b": 2}')`,
	} {
		require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(stmt, srcFullName)))
	}

	jsonColumns := []string{"c_scalar", "c_array", "c_obj"}
	EnvWaitForEqualTablesWithNamesFunc(env, s, "waiting on cdc add column with default", srcTableName, dstTableName,
		"id,"+strings.Join(jsonColumns, ","), jsonBatchesMatch(jsonColumns, nil, true))

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

// jsonColumnTestCases is the source value table shared by the JSON column tests. mysqlUtf8mb4Support
// drops the emoji case on MySQL servers that do not default to utf8mb4.
func jsonColumnTestCases(mysqlUtf8mb4Support bool) []jsonColumnTestCase {
	// TODO: fix and enable failing test cases
	return []jsonColumnTestCase{
		{
			desc:  "obj",
			value: `'{"key": "val", "number": 30, "bool": true}'`,
		},
		{
			desc:  "nested obj",
			value: `'{"name": "john", "contact": {"email": "john@example.com", "phone": {"cell": "1234567890"}}}'`,
		},
		{
			desc:  "obj with array as values",
			value: `'{"array_key": [1, 2, 3, ["hello", "world"]], "empty_array": []}'`,
		},
		{
			desc:  "obj with numeric values",
			value: `'{"int": 42, "float": 3.14159, "scientific": 1.23e-4, "negative": -456}'`,
		},
		{
			desc:  "obj with special chars as values",
			value: `'{"path": "/home/user","quote": "check \"quoted\"", "newline": "line1\nline2"}'`,
		},
		{
			desc:  "obj with emoji",
			value: `'{"unicode": "🚀"}'`,
			skip:  !mysqlUtf8mb4Support,
		},
		{
			desc:  "empty object",
			value: `'{}'`,
		},
		{
			desc:  "same key as int",
			value: `'{"v": 1}'`,
		},
		{
			desc:  "same key as string",
			value: `'{"v": "1"}'`,
		},
		{
			desc:  "same key as float",
			value: `'{"v": 1.5}'`,
		},
		{
			desc:  "same key as array",
			value: `'{"v": [1]}'`,
		},
		{
			desc:  "same key as object",
			value: `'{"v": {"x": 1}}'`,
		},
		{
			desc:  "same key as bool",
			value: `'{"v": true}'`,
		},
		{
			desc:  "array with null element",
			value: `'{"arr": [1, null, 2]}'`,
		},
		{
			desc:  "array with mixed types",
			value: `'{"arr": [1, "a", true, {"k": 1}, [2]]}'`,
		},
		{
			desc:  "float edges",
			value: `'{"exp": 2.5e10, "negexp": 1.5e-5, "negzero": -0.0, "tenth": 0.1}'`,
		},
		{
			desc:  "awkward keys",
			value: `'{"": 1, "with space": 2, "qu\"ote": 3, "ключ": 4, "1": 5}'`,
		},
		{
			desc:  "awkward string values",
			value: `'{"empty": "", "spaces": "   ", "escaped": "\u00e9", "literal": "é", "tab": "a\tb"}'`,
		},
		{
			desc:  "numeric-looking strings",
			value: `'{"zip": "00123", "sci": "1e5", "bool": "true", "null": "null"}'`,
		},
		{
			desc:  "many keys",
			value: manyKeysJSON(500),
		},
		{
			desc:  "deep nesting",
			value: deeplyNestedJSON(30),
		},
		{
			desc:  "large string value",
			value: `'{"blob": "` + strings.Repeat("x", 1<<20) + `"}'`,
		},
		{
			desc:  "top-level number",
			value: `'123'`,
		},
		{
			desc:  "top-level string",
			value: `'"string value"'`,
		},
		{
			desc:  "top-level boolean",
			value: `'true'`,
		},
		{
			desc:  "top-level arrays",
			value: `'[1, 2, 3, {"key": "val"}, ["hello", "world"]]'`,
		},
		{
			desc:  "json null value",
			value: `'null'`,
		},
		{
			desc:   "postgres null value",
			value:  `null`,
			isNull: true,
		},
		{
			desc:       "null as values",
			value:      `'{"key1": null, "key2": {"nested_key": null}}'`,
			skip:       true,
			skipReason: "insertion succeeds; but comparison fails because CH JSON removes all null fields but postgres does not",
		},
	}
}

// testJSONColumns mirrors a table with JSON columns (json and jsonb on Postgres, json on MySQL)
// into ClickHouse, asserts the destination column type, and checks every destination row against
// the source for the snapshot and the CDC path.
func (s ClickHouseSuite) testJSONColumns(cfg jsonColumnMirrorConfig) {
	mysqlUtf8mb4Support := true
	if mySource, ok := s.source.(*MySqlSource); ok {
		if mySource.Config.Flavor == protos.MySqlFlavor_MYSQL_MARIA {
			if cfg.nativeJSON {
				s.t.Skip("skip maria, where JSON is stored as LONGTEXT and PeerDB does not map it to JSON yet")
			}
		} else {
			cmp, err := mySource.CompareServerVersion(s.t.Context(), "8")
			require.NoError(s.t, err)
			if cmp < 0 {
				mysqlUtf8mb4Support = false
			}
		}
	}

	if _, isPostgres := s.source.(*PostgresSource); cfg.disableFastProcessJSONColumns && !isPostgres {
		s.t.Skip("PEERDB_POSTGRES_FAST_PROCESS_JSON_COLUMNS only applies to postgres")
	}

	jsonTestCases := jsonColumnTestCases(mysqlUtf8mb4Support)
	// with native JSON on, the readers wrap non-object documents
	wrapNonObjects := cfg.nativeJSON

	srcTableName := cfg.tableName
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := cfg.tableName + "_dst"

	jsonColumns := []string{"json"}
	createStmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, json JSON);`, srcFullName)
	insertStmt := func(v string) string { return fmt.Sprintf(`INSERT INTO %s (json) VALUES (%s)`, srcFullName, v) }
	if _, isPostgres := s.source.(*PostgresSource); isPostgres {
		jsonColumns = []string{"json", "jsonb"}
		createStmt = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, json JSON, jsonb JSONB);`, srcFullName)
		insertStmt = func(v string) string {
			return fmt.Sprintf(`INSERT INTO %s (json, jsonb) VALUES (%s, %s)`, srcFullName, v, v)
		}
	}

	require.NoError(s.T(), s.source.Exec(s.t.Context(), createStmt))
	for _, testCase := range jsonTestCases {
		if !testCase.skip {
			require.NoError(s.t, s.source.Exec(s.t.Context(), insertStmt(testCase.value)))
		}
	}
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(cfg.tableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{
		"PEERDB_CLICKHOUSE_ENABLE_JSON": strconv.FormatBool(cfg.nativeJSON),
		"PEERDB_NULLABLE":               strconv.FormatBool(cfg.nullableDestinationColumns),
	}
	if cfg.disableFastProcessJSONColumns {
		flowConnConfig.Env["PEERDB_POSTGRES_FAST_PROCESS_JSON_COLUMNS"] = "false"
	}

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	// destination tables exist once the flow is running, so check their type before waiting on rows;
	// a wrong type never changes, so fail now rather than after the row wait times out, cancelling
	// the mirror first so teardown can drop the source schema
	for _, column := range jsonColumns {
		columnType, err := s.GetColumnType(dstTableName, column)
		if err != nil || columnType != cfg.expectedColumnType {
			env.Cancel(s.t.Context())
		}
		require.NoError(s.t, err)
		require.Equal(s.t, cfg.expectedColumnType, columnType, "unexpected type for column %s.%s", dstTableName, column)
	}

	cols := "id," + strings.Join(jsonColumns, ",")
	rowsMatch := jsonBatchesMatch(jsonColumns, cfg.sourceNullLandsAs, wrapNonObjects)
	EnvWaitForEqualTablesWithNamesFunc(env, s, "compare initial load", srcTableName, dstTableName, cols, rowsMatch)

	for _, testCase := range jsonTestCases {
		if testCase.skip {
			continue
		}
		require.NoError(s.t, s.source.Exec(s.t.Context(), insertStmt(testCase.value)))
	}
	EnvWaitForEqualTablesWithNamesFunc(env, s, "compare cdc", srcTableName, dstTableName, cols, rowsMatch)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

// jsonColumnMirrorConfig parameterizes testJSONColumns over the PEERDB_CLICKHOUSE_ENABLE_JSON and
// PEERDB_NULLABLE settings.
type jsonColumnMirrorConfig struct {
	// sourceNullLandsAs is what a source SQL NULL becomes on the destination; nil means it stays NULL
	sourceNullLandsAs  *string
	tableName          string
	expectedColumnType string
	// nativeJSON sets PEERDB_CLICKHOUSE_ENABLE_JSON
	nativeJSON bool
	// nullableDestinationColumns sets PEERDB_NULLABLE, so nullable source columns get Nullable(...) on the destination
	nullableDestinationColumns bool
	// disableFastProcessJSONColumns sets PEERDB_POSTGRES_FAST_PROCESS_JSON_COLUMNS to false, so the Postgres reader
	// takes the unmarshal/marshal path instead of the token walk; Postgres only
	disableFastProcessJSONColumns bool
}

// jsonColumnTestCase is one source value for the JSON column tests. value is the SQL literal to insert.
type jsonColumnTestCase struct {
	value      string
	desc       string
	skipReason string
	skip       bool
	isNull     bool
}

// manyKeysJSON returns '{"k0": 0, "k1": 1, ..., "k<n-1>": <n-1>}' as a SQL literal.
// Keep n below ClickHouse's default max_dynamic_paths (1024); clickhouse-go cannot decode the overflow yet.
func manyKeysJSON(n int) string {
	var b strings.Builder
	b.WriteString("'{")
	for i := range n {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `"k%d": %d`, i, i)
	}
	b.WriteString("}'")
	return b.String()
}

// deeplyNestedJSON returns '{"n": {"n": {"n": 1}}}' with depth levels, as a SQL literal.
func deeplyNestedJSON(depth int) string {
	return "'" + strings.Repeat(`{"n": `, depth) + "1" + strings.Repeat("}", depth) + "'"
}

func truncateForLog(s string) string {
	if len(s) > 500 {
		return s[:500] + "...[truncated]"
	}
	return s
}

// destinationJSONMatches reports whether a destination cell holds what its source cell should have
// produced: an equal JSON document (compared parsed, so key order and whitespace do not matter), or
// sourceNullLandsAs when the source is SQL NULL (nil means it stays NULL). With wrapNonObjects the source
// document is first wrapped the way the readers do it. label names the cell in logs.
func destinationJSONMatches(
	t *testing.T, label string, src types.QValue, dst types.QValue, sourceNullLandsAs *string, wrapNonObjects bool,
) bool {
	t.Helper()
	srcVal, dstVal := src.Value(), dst.Value()
	if srcVal == nil {
		var want any
		if sourceNullLandsAs != nil {
			want = *sourceNullLandsAs
		}
		if dstVal != want {
			t.Logf("%s: source NULL should land as %#v, got %#v", label, want, dstVal)
		}
		return dstVal == want
	}
	srcText, srcOK := srcVal.(string)
	dstText, dstOK := dstVal.(string)
	if !srcOK || !dstOK {
		t.Logf("%s: expected JSON text on both sides, got %T and %T", label, srcVal, dstVal)
		return false
	}
	if wrapNonObjects {
		srcText = qvalue.WrapNonObjectJSON(srcText)
	}
	var srcParsed, dstParsed any
	srcErr := json.Unmarshal([]byte(srcText), &srcParsed)
	dstErr := json.Unmarshal([]byte(dstText), &dstParsed)
	if srcErr != nil || dstErr != nil {
		t.Logf("%s: invalid JSON: source %v, destination %v", label, srcErr, dstErr)
		return false
	}
	if wrapNonObjects {
		// ClickHouse JSON drops paths whose value is null, so a wrapped JSON null reads back as {}
		if m, ok := srcParsed.(map[string]any); ok && len(m) == 1 {
			if v, has := m["_value"]; has && v == nil {
				srcParsed = map[string]any{}
			}
		}
	}
	if !reflect.DeepEqual(srcParsed, dstParsed) {
		t.Logf("%s: %s != %s", label, truncateForLog(srcText), truncateForLog(dstText))
		return false
	}
	return true
}

// jsonBatchesMatch returns a batch comparison for EnvWaitForEqualTablesWithNamesFunc over tables selected
// as "id" followed by jsonColumns, so row i and column c+1 line up on both sides. wrapNonObjects says
// whether the mirror wraps non-object documents.
func jsonBatchesMatch(
	jsonColumns []string, sourceNullLandsAs *string, wrapNonObjects bool,
) func(*testing.T, *model.QRecordBatch, *model.QRecordBatch) bool {
	return func(t *testing.T, src *model.QRecordBatch, dst *model.QRecordBatch) bool {
		t.Helper()
		if len(dst.Records) != len(src.Records) {
			t.Logf("row count %d != %d", len(dst.Records), len(src.Records))
			return false
		}
		for rowIdx, srcRow := range src.Records {
			if !qvalue.Equals(srcRow[0], dst.Records[rowIdx][0]) {
				t.Logf("row %d: id %v != %v", rowIdx, srcRow[0].Value(), dst.Records[rowIdx][0].Value())
				return false
			}
			for jsonColIdx, column := range jsonColumns {
				colIdx := jsonColIdx + 1 // column 0 is id
				label := fmt.Sprintf("row %d %s", rowIdx, column)
				if !destinationJSONMatches(t, label, srcRow[colIdx], dst.Records[rowIdx][colIdx], sourceNullLandsAs, wrapNonObjects) {
					return false
				}
			}
		}
		return true
	}
}
