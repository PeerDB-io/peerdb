package e2e

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
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

// Test_JSON_CH_JSONOverride keeps PEERDB_CLICKHOUSE_ENABLE_JSON off and pins the JSON columns to JSON through
// their destination type: the override alone selects native JSON, and the readers wrap for it.
func (s ClickHouseSuite) Test_JSON_CH_JSONOverride() {
	emptyObject := "{}"
	s.testJSONColumns(jsonColumnMirrorConfig{
		tableName:          "test_json_override",
		overrideToJSON:     true,
		expectedColumnType: "JSON",
		sourceNullLandsAs:  &emptyObject,
	})
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

// testJSONColumns mirrors a table with JSON columns (json and jsonb on Postgres, json on MySQL)
// into ClickHouse, asserts the destination column type, and checks every destination row against
// the source for the snapshot and the CDC path.
func (s ClickHouseSuite) testJSONColumns(cfg jsonColumnMirrorConfig) {
	mysqlUtf8mb4Support := true
	if mySource, ok := s.source.(*MySqlSource); ok && mySource.Config.Flavor != protos.MySqlFlavor_MYSQL_MARIA {
		cmp, err := mySource.CompareServerVersion(s.t.Context(), "8")
		require.NoError(s.t, err)
		if cmp < 0 {
			mysqlUtf8mb4Support = false
		}
	}

	if _, isPostgres := s.source.(*PostgresSource); cfg.disableFastProcessJSONColumns && !isPostgres {
		s.t.Skip("PEERDB_POSTGRES_FAST_PROCESS_JSON_COLUMNS only applies to postgres")
	}

	// source values; the emoji case is dropped on MySQL servers that do not default to utf8mb4
	jsonTestCases := []jsonColumnTestCase{
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
			desc:  "top-level false",
			value: `'false'`,
		},
		{
			desc:  "top-level negative float",
			value: `'-1.5'`,
		},
		{
			desc:  "top-level empty string",
			value: `'""'`,
		},
		{
			desc:  "top-level string starting with a brace",
			value: `'"{not an object"'`,
		},
		{
			desc:  "top-level empty array",
			value: `'[]'`,
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
	// with native JSON on, or the columns pinned to JSON, the readers wrap non-object documents
	wrapNonObjects := cfg.nativeJSON || cfg.overrideToJSON

	srcTableName := cfg.tableName
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := cfg.tableName + "_dst"

	jsonColumns := []string{"json"}
	createStmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, json JSON);`, srcFullName)
	insertStmt := func(v string) string { return fmt.Sprintf(`INSERT INTO %s (json) VALUES (%s)`, srcFullName, v) }
	if _, isMySQL := s.source.(*MySqlSource); isMySQL {
		// test values are standard SQL literals, but MySQL reads backslash as an escape character
		insertStmt = func(v string) string {
			return fmt.Sprintf(`INSERT INTO %s (json) VALUES (%s)`, srcFullName, strings.ReplaceAll(v, `\`, `\\`))
		}
	}
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
		"PEERDB_NULLABLE": strconv.FormatBool(cfg.nullableDestinationColumns),
	}
	if cfg.nativeJSON {
		flowConnConfig.Env["PEERDB_CLICKHOUSE_ENABLE_JSON"] = "true"
	}
	if cfg.overrideToJSON {
		for _, column := range jsonColumns {
			flowConnConfig.TableMappings[0].Columns = append(flowConnConfig.TableMappings[0].Columns,
				&protos.ColumnSetting{SourceName: column, DestinationType: "JSON"})
		}
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
	nativeJSON         bool
	// nullableDestinationColumns sets PEERDB_NULLABLE, so nullable source columns get Nullable(...) on the destination
	nullableDestinationColumns bool
	// overrideToJSON gives every JSON column the destination type JSON through its column settings
	overrideToJSON bool
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
	return jsonColumnsMatch(jsonColumns, func(t *testing.T, label string, _ int, src types.QValue, dst types.QValue) bool {
		t.Helper()
		return destinationJSONMatches(t, label, src, dst, sourceNullLandsAs, wrapNonObjects)
	})
}

// jsonColumnsMatch returns a batch comparison over tables selected as "id" followed by columns: the ids must line
// up row by row, and cellMatches decides each other cell, given its row index.
func jsonColumnsMatch(
	columns []string, cellMatches func(t *testing.T, label string, rowIdx int, src types.QValue, dst types.QValue) bool,
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
			for jsonColIdx, column := range columns {
				colIdx := jsonColIdx + 1 // column 0 is id
				label := fmt.Sprintf("row %d %s", rowIdx, column)
				if !cellMatches(t, label, rowIdx, srcRow[colIdx], dst.Records[rowIdx][colIdx]) {
					return false
				}
			}
		}
		return true
	}
}

// Test_JSON_CH_OldMirror runs a native JSON mirror as one created before InternalVersion_WrapNonObjectJSON:
// object documents still land unchanged, and a non-object document is still rejected by ClickHouse and
// surfaces as a flow error, since the reader does not wrap for such a mirror.
func (s ClickHouseSuite) Test_JSON_CH_OldMirror() {
	if mySource, ok := s.source.(*MySqlSource); ok && mySource.Config.Flavor == protos.MySqlFlavor_MYSQL_MARIA {
		s.t.Skip("a mirror below InternalVersion_MariaDBJSONColumns types a MariaDB JSON column as String, so there is no JSON column to test")
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

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (json) VALUES ('"dark"')`, srcFullName)))
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for the unwrapped scalar to fail", func() bool {
		// casting a non-object to JSON fails with INCORRECT_DATA
		count, err := GetLogCount(s.t.Context(), s.catalog, flowConnConfig.FlowJobName, "error",
			fmt.Sprintf("code: %d", chproto.ErrIncorrectData))
		return err == nil && count > 0
	})

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
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

// Test_JSON_CH_StringOverride pins a JSON column to String on a native JSON mirror. The snapshot has to
// leave that column out of its JSON cast, as the DDL and the normalize query already honour the override.
func (s ClickHouseSuite) Test_JSON_CH_StringOverride() {
	srcTableName := "test_json_string_override"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := srcTableName + "_dst"
	docs := []string{`'{"key": "val"}'`, `'"dark"'`, `'[1, 2]'`, `'42'`}
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
	flowConnConfig.Env = map[string]string{"PEERDB_CLICKHOUSE_ENABLE_JSON": "true"}
	flowConnConfig.TableMappings[0].Columns = []*protos.ColumnSetting{{SourceName: "json", DestinationType: "String"}}

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	columnType, err := s.GetColumnType(dstTableName, "json")
	require.NoError(s.t, err)
	require.Equal(s.t, "String", columnType)

	rowsMatch := jsonBatchesMatch([]string{"json"}, nil, false)
	EnvWaitForEqualTablesWithNamesFunc(env, s, "compare initial load", srcTableName, dstTableName, "id,json", rowsMatch)
	insertDocs()
	EnvWaitForEqualTablesWithNamesFunc(env, s, "compare cdc", srcTableName, dstTableName, "id,json", rowsMatch)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

// Postgres json[]/jsonb[] into ClickHouse: Array(JSON) by default with native JSON on, Array(Nullable(JSON)) with
// PEERDB_NULLABLE, explicit Array types as overrides, String otherwise.

// Test_JSON_CH_Array: with native JSON on, json[]/jsonb[] land in Array(JSON) by default.
func (s ClickHouseSuite) Test_JSON_CH_Array() {
	s.testJSONArrayColumns(jsonArrayMirrorConfig{
		tableName:          "test_json_arr",
		nativeJSON:         true,
		expectedColumnType: "Array(JSON)",
	})
}

// Test_JSON_CH_Array_ClassicReader is Test_JSON_CH_Array with the Postgres reader taking the unmarshal/marshal path.
func (s ClickHouseSuite) Test_JSON_CH_Array_ClassicReader() {
	s.testJSONArrayColumns(jsonArrayMirrorConfig{
		tableName:                     "test_json_arr_classic",
		nativeJSON:                    true,
		disableFastProcessJSONColumns: true,
		expectedColumnType:            "Array(JSON)",
	})
}

// Test_JSON_CH_Array_Nullable: with PEERDB_NULLABLE a SQL NULL element stays NULL; a SQL NULL array is still [].
func (s ClickHouseSuite) Test_JSON_CH_Array_Nullable() {
	s.testJSONArrayColumns(jsonArrayMirrorConfig{
		tableName:                  "test_json_arr_nullable",
		nativeJSON:                 true,
		nullableDestinationColumns: true,
		expectedColumnType:         "Array(Nullable(JSON))",
	})
}

// Test_JSON_CH_Array_Disabled: with native JSON off the whole array lands as one String.
func (s ClickHouseSuite) Test_JSON_CH_Array_Disabled() {
	s.testJSONArrayColumns(jsonArrayMirrorConfig{
		tableName:          "test_json_arr_off",
		expectedColumnType: "String",
	})
}

// Test_JSON_CH_Array_OldMirror: a mirror from before InternalVersion_JSONArrayAsArrayJSON keeps String.
func (s ClickHouseSuite) Test_JSON_CH_Array_OldMirror() {
	s.testJSONArrayColumns(jsonArrayMirrorConfig{
		tableName:          "test_json_arr_old",
		nativeJSON:         true,
		version:            shared.InternalVersion_JSONArrayAsArrayJSON - 1,
		expectedColumnType: "String",
	})
}

// Test_JSON_CH_Array_Override: an Array(JSON) override gets wrapped elements.
func (s ClickHouseSuite) Test_JSON_CH_Array_Override() {
	s.testJSONArrayColumns(jsonArrayMirrorConfig{
		tableName:          "test_json_arr_override",
		overrideType:       "Array(JSON)",
		expectedColumnType: "Array(JSON)",
	})
}

// Test_JSON_CH_Array_OverrideNullable: an Array(Nullable(JSON)) override keeps SQL NULL elements as NULL.
func (s ClickHouseSuite) Test_JSON_CH_Array_OverrideNullable() {
	s.testJSONArrayColumns(jsonArrayMirrorConfig{
		tableName:          "test_json_arr_override_nullable",
		overrideType:       "Array(Nullable(JSON))",
		expectedColumnType: "Array(Nullable(JSON))",
	})
}

// Test_JSON_CH_Array_OverrideNullable_ClassicReader: the same with fast JSON processing off.
func (s ClickHouseSuite) Test_JSON_CH_Array_OverrideNullable_ClassicReader() {
	s.testJSONArrayColumns(jsonArrayMirrorConfig{
		tableName:                     "test_json_arr_nullable_classic",
		overrideType:                  "Array(Nullable(JSON))",
		disableFastProcessJSONColumns: true,
		expectedColumnType:            "Array(Nullable(JSON))",
	})
}

// Test_JSON_CH_Array_Override_OldMirror: a mirror from before the version keeps elements unwrapped.
func (s ClickHouseSuite) Test_JSON_CH_Array_Override_OldMirror() {
	s.testJSONArrayColumns(jsonArrayMirrorConfig{
		tableName:          "test_json_arr_override_old",
		overrideType:       "Array(JSON)",
		version:            shared.InternalVersion_WrapJSONArrayElements - 1,
		expectedColumnType: "Array(JSON)",
	})
}

// Test_JSON_CH_Array_Domain: columns declared through domains over json[]/jsonb[] map and wrap like the base types.
// CDC sees the domain's OID, while the snapshot sees the base type's, so both paths must resolve the domain alike.
func (s ClickHouseSuite) Test_JSON_CH_Array_Domain() {
	s.testJSONArrayColumns(jsonArrayMirrorConfig{
		tableName:          "test_json_arr_domain",
		nativeJSON:         true,
		domainColumns:      true,
		expectedColumnType: "Array(JSON)",
	})
}

// jsonArrayMirrorConfig parameterizes testJSONArrayColumns.
type jsonArrayMirrorConfig struct {
	tableName          string
	expectedColumnType string
	// overrideType is given to every array column as its destination type through the column settings
	overrideType string
	// version, when set, pins the mirror to that internal version through PEERDB_FORCE_INTERNAL_VERSION
	version    uint32
	nativeJSON bool
	// nullableDestinationColumns sets PEERDB_NULLABLE
	nullableDestinationColumns bool
	// disableFastProcessJSONColumns sets PEERDB_POSTGRES_FAST_PROCESS_JSON_COLUMNS to false, so the Postgres reader
	// takes the unmarshal/marshal path instead of the token walk
	disableFastProcessJSONColumns bool
	// domainColumns declares the columns through domains over json[] and jsonb[]
	domainColumns bool
}

// jsonArrayTestCase is one json[]/jsonb[] value. elements are the JSON documents of the Postgres array, nil for a
// SQL NULL element; isNull makes the whole column SQL NULL. wantElements is what Array(JSON) holds, spelled out
// rather than derived with the production wrap helper; wantNullableElements is what Array(Nullable(JSON)) holds,
// set only where it differs.
type jsonArrayTestCase struct {
	desc                 string
	elements             []*string
	wantElements         []string
	wantNullableElements []string
	isNull               bool
}

// allElementsAreObjects reports whether every element is an object document, so that the destination holds the
// elements as they are whether or not they were wrapped.
func (tc jsonArrayTestCase) allElementsAreObjects() bool {
	for _, elem := range tc.elements {
		if elem == nil || !strings.HasPrefix(strings.TrimSpace(*elem), "{") {
			return false
		}
	}
	return true
}

func jsonArrayTestCases() []jsonArrayTestCase {
	return []jsonArrayTestCase{
		{
			desc:         "objects",
			elements:     []*string{new(`{"a": 1}`), new(`{"b": {"c": [1, 2]}, "s": "x"}`)},
			wantElements: []string{`{"a": 1}`, `{"b": {"c": [1, 2]}, "s": "x"}`},
		},
		{
			desc:         "empty object",
			elements:     []*string{new(`{}`)},
			wantElements: []string{`{}`},
		},
		{
			desc:         "scalars",
			elements:     []*string{new(`"text"`), new(`42`), new(`true`), new(`-1.5`), new(`""`)},
			wantElements: []string{`{"_value":"text"}`, `{"_value":42}`, `{"_value":true}`, `{"_value":-1.5}`, `{"_value":""}`},
		},
		{
			desc:         "string starting with a brace",
			elements:     []*string{new(`"{not an object"`)},
			wantElements: []string{`{"_value":"{not an object"}`},
		},
		{
			desc:         "nested arrays",
			elements:     []*string{new(`[1, 2]`), new(`[[3], {"k": "v"}]`), new(`[]`)},
			wantElements: []string{`{"_value":[1, 2]}`, `{"_value":[[3], {"k": "v"}]}`, `{"_value":[]}`},
		},
		{
			desc:                 "null element",
			elements:             []*string{new(`{"a": 1}`), nil, new(`"s"`)},
			wantElements:         []string{`{"a": 1}`, `{}`, `{"_value":"s"}`},
			wantNullableElements: []string{`{"a": 1}`, `null`, `{"_value":"s"}`},
		},
		{
			desc:         "json null element",
			elements:     []*string{new(`null`), new(`{"a": 1}`)},
			wantElements: []string{`{}`, `{"a": 1}`},
		},
		{
			// JSONExtract alone would parse [null,null] as the empty Array(JSON)
			desc:                 "all null elements",
			elements:             []*string{nil, nil},
			wantElements:         []string{`{}`, `{}`},
			wantNullableElements: []string{`null`, `null`},
		},
		{
			desc:         "unicode and escapes",
			elements:     []*string{new(`{"k": "日本語 🚀 \"q\" \\ \n"}`), new(`"é\ttab"`)},
			wantElements: []string{`{"k": "日本語 🚀 \"q\" \\ \n"}`, `{"_value":"é\ttab"}`},
		},
		{
			desc:         "empty array",
			elements:     []*string{},
			wantElements: []string{},
		},
		{
			desc:         "sql null column",
			isNull:       true,
			wantElements: []string{},
		},
	}
}

// sqlLiteral renders the case as a Postgres array literal of the given element type, json or jsonb.
func (tc jsonArrayTestCase) sqlLiteral(elementType string) string {
	if tc.isNull {
		return "NULL"
	}
	elems := make([]string, len(tc.elements))
	for i, elem := range tc.elements {
		if elem == nil {
			elems[i] = "NULL"
		} else {
			elems[i] = "'" + strings.ReplaceAll(*elem, "'", "''") + "'::" + elementType
		}
	}
	return fmt.Sprintf("ARRAY[%s]::%s[]", strings.Join(elems, ", "), elementType)
}

func (s ClickHouseSuite) testJSONArrayColumns(cfg jsonArrayMirrorConfig) {
	if _, isPostgres := s.source.(*PostgresSource); !isPostgres {
		s.t.Skip("json arrays only exist on postgres")
	}

	srcTableName := cfg.tableName
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := cfg.tableName + "_dst"
	arrayColumns := []string{"json_arr", "jsonb_arr"}
	cases := jsonArrayTestCases()

	jsonArrayType, jsonbArrayType := "JSON[]", "JSONB[]"
	if cfg.domainColumns {
		jsonArrayType = s.attachSchemaSuffix(srcTableName + "_json_dom")
		jsonbArrayType = s.attachSchemaSuffix(srcTableName + "_jsonb_dom")
		require.NoError(s.t, s.source.Exec(s.t.Context(), "CREATE DOMAIN "+jsonArrayType+" AS json[]"))
		require.NoError(s.t, s.source.Exec(s.t.Context(), "CREATE DOMAIN "+jsonbArrayType+" AS jsonb[]"))
	}
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, json_arr %s, jsonb_arr %s)`,
		srcFullName, jsonArrayType, jsonbArrayType)))
	insertCases := func() {
		for _, tc := range cases {
			require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (json_arr, jsonb_arr) VALUES (%s, %s)`,
				srcFullName, tc.sqlLiteral("json"), tc.sqlLiteral("jsonb"))), tc.desc)
		}
	}
	insertCases()

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
	if cfg.overrideType != "" {
		for _, column := range arrayColumns {
			flowConnConfig.TableMappings[0].Columns = append(flowConnConfig.TableMappings[0].Columns,
				&protos.ColumnSetting{SourceName: column, DestinationType: cfg.overrideType})
		}
	}
	if cfg.version != 0 {
		flowConnConfig.Version = cfg.version
		flowConnConfig.Env["PEERDB_FORCE_INTERNAL_VERSION"] = strconv.FormatUint(uint64(flowConnConfig.Version), 10)
	}
	if cfg.disableFastProcessJSONColumns {
		flowConnConfig.Env["PEERDB_POSTGRES_FAST_PROCESS_JSON_COLUMNS"] = "false"
	}

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	// a wrong type never changes, so fail now rather than after the row wait times out
	for _, column := range arrayColumns {
		columnType, err := s.GetColumnType(dstTableName, column)
		if err != nil || columnType != cfg.expectedColumnType {
			env.Cancel(s.t.Context())
		}
		require.NoError(s.t, err)
		require.Equal(s.t, cfg.expectedColumnType, columnType, "unexpected type for column %s.%s", dstTableName, column)
	}

	cols := "id," + strings.Join(arrayColumns, ",")
	// rows are ordered by id and the cases were inserted in order, once per load, so row i is cases[i % len(cases)]
	elementsWrapped := cfg.version == 0 || cfg.version >= shared.InternalVersion_WrapJSONArrayElements
	rowsMatch := jsonColumnsMatch(arrayColumns, func(t *testing.T, label string, rowIdx int, _ types.QValue, dst types.QValue) bool {
		t.Helper()
		tc := cases[rowIdx%len(cases)]
		return destinationJSONArrayMatches(t, label+" ("+tc.desc+")", tc, dst, cfg.expectedColumnType, elementsWrapped)
	})
	if cfg.expectedColumnType == "String" {
		// a String column holds the source array text as it is, and '' for a SQL NULL array
		emptyString := ""
		rowsMatch = jsonBatchesMatch(arrayColumns, &emptyString, false)
	}
	EnvWaitForEqualTablesWithNamesFunc(env, s, "compare initial load", srcTableName, dstTableName, cols, rowsMatch)

	insertCases()
	EnvWaitForEqualTablesWithNamesFunc(env, s, "compare cdc", srcTableName, dstTableName, cols, rowsMatch)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

// destinationJSONArrayMatches checks one Array(JSON) or Array(Nullable(JSON)) cell against its test case's
// wantElements.
func destinationJSONArrayMatches(
	t *testing.T, label string, tc jsonArrayTestCase, dst types.QValue, columnType string, elementsWrapped bool,
) bool {
	t.Helper()
	dstArray, ok := dst.(types.QValueArrayJSON)
	if !ok {
		t.Logf("%s: expected an Array(JSON) value, got %T", label, dst)
		return false
	}
	var dstElements []json.RawMessage
	if err := json.Unmarshal([]byte(dstArray.Val), &dstElements); err != nil {
		t.Logf("%s: invalid destination JSON array %s: %v", label, truncateForLog(dstArray.Val), err)
		return false
	}
	wantElements := tc.wantElements
	if columnType == "Array(Nullable(JSON))" && tc.wantNullableElements != nil {
		wantElements = tc.wantNullableElements
	}
	if !elementsWrapped {
		// A mirror from before InternalVersion_WrapJSONArrayElements ships the elements as they are. What
		// ClickHouse makes of a non-object element in Array(JSON) ({} or dropped) is its own business and differs
		// by version, so only pin that nothing was wrapped and that an array of objects arrives as it is.
		if strings.Contains(dstArray.Val, `"_value"`) {
			t.Logf("%s: elements wrapped on an old mirror: %s", label, truncateForLog(dstArray.Val))
			return false
		}
		if !tc.allElementsAreObjects() {
			return true
		}
	}
	if len(dstElements) != len(wantElements) {
		t.Logf("%s: %d elements, want %d: %s", label, len(dstElements), len(wantElements), truncateForLog(dstArray.Val))
		return false
	}
	for i, want := range wantElements {
		if !destinationJSONMatches(t, fmt.Sprintf("%s element %d", label, i),
			types.QValueJSON{Val: want}, types.QValueJSON{Val: string(dstElements[i])}, nil, false) {
			return false
		}
	}
	return true
}

// Test_JSON_CH_Array_AddColumnDefault: a jsonb[] column added with a default, older rows get the wrapped default.
func (s ClickHouseSuite) Test_JSON_CH_Array_AddColumnDefault() {
	s.testJSONArrayAddColumnDefault("test_json_arr_add_col_default", false)
}

// Test_JSON_CH_Array_AddColumnDefault_Nullable: the same with PEERDB_NULLABLE; a NULL element of the default lands as {}.
func (s ClickHouseSuite) Test_JSON_CH_Array_AddColumnDefault_Nullable() {
	s.testJSONArrayAddColumnDefault("test_json_arr_add_col_def_null", true)
}

func (s ClickHouseSuite) testJSONArrayAddColumnDefault(srcTableName string, nullable bool) {
	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("json arrays only exist on postgres")
	}

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
	flowConnConfig.Env = map[string]string{
		"PEERDB_CLICKHOUSE_ENABLE_JSON": "true",
		"PEERDB_NULLABLE":               strconv.FormatBool(nullable),
	}
	expectedColumnType := "Array(JSON)"
	if nullable {
		expectedColumnType = "Array(Nullable(JSON))"
	}

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id")

	for _, stmt := range []string{
		`ALTER TABLE %s ADD COLUMN c_arr JSONB[] DEFAULT ARRAY['{"a": 1}'::jsonb, '"dark"'::jsonb, NULL, 'null'::jsonb]`,
		`INSERT INTO %s (id, c_arr) VALUES (2, ARRAY['[1, 2]'::jsonb, '{"b": 2}'::jsonb])`,
	} {
		require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(stmt, srcFullName)))
	}

	// row 1 takes the column default, row 2 its own value
	cases := []jsonArrayTestCase{
		{desc: "default", wantElements: []string{`{"a": 1}`, `{"_value":"dark"}`, `{}`, `{}`}},
		{desc: "explicit", wantElements: []string{`{"_value":[1, 2]}`, `{"b": 2}`}},
	}
	rowsMatch := jsonColumnsMatch([]string{"c_arr"}, func(t *testing.T, label string, rowIdx int, _ types.QValue, dst types.QValue) bool {
		t.Helper()
		tc := cases[rowIdx]
		return destinationJSONArrayMatches(t, label+" ("+tc.desc+")", tc, dst, expectedColumnType, true)
	})
	EnvWaitForEqualTablesWithNamesFunc(env, s, "waiting on cdc add column with default", srcTableName, dstTableName,
		"id,c_arr", rowsMatch)

	columnType, err := s.GetColumnType(dstTableName, "c_arr")
	require.NoError(s.t, err)
	require.Equal(s.t, expectedColumnType, columnType)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}
