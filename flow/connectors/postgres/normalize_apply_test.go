//go:build tilt

package connpostgres

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

type applyCase struct {
	table  string
	ddl    string
	soft   bool
	full   bool
	keys   []string
	cols   [][2]string
	raw    []string
	expect string
}

func applySchema(c applyCase) *protos.TableSchema {
	fields := make([]*protos.FieldDescription, 0, len(c.cols))
	for _, col := range c.cols {
		fields = append(fields, &protos.FieldDescription{Name: col[0], Type: col[1], TypeModifier: -1})
	}
	keys := c.keys
	if keys == nil {
		for _, col := range c.cols {
			keys = append(keys, col[0])
		}
	}
	return &protos.TableSchema{TableIdentifier: c.table, PrimaryKeyColumns: keys, IsReplicaIdentityFull: c.full,
		Columns: fields, System: protos.TypeSystem_PG}
}

var applyCases = []applyCase{
	{table: "sw", ddl: "create table sw(id int primary key, code text not null unique); insert into sw values (1,'a'),(2,'b')",
		keys: []string{"id"}, cols: [][2]string{{"id", "int4"}, {"code", "text"}},
		raw:    []string{`1|{"id":"1","code":"tmp"}|{}|`, `1|{"id":"2","code":"a"}|{}|`, `1|{"id":"1","code":"b"}|{}|`},
		expect: "1:b 2:a"},
	{table: "sh", ddl: "create table sh(id int primary key, pos int not null unique); insert into sh values (1,1),(2,2),(3,3)",
		keys: []string{"id"}, cols: [][2]string{{"id", "int4"}, {"pos", "int4"}},
		raw: []string{`1|{"id":"1","pos":"-1"}|{}|`, `1|{"id":"2","pos":"-2"}|{}|`, `1|{"id":"3","pos":"-3"}|{}|`,
			`1|{"id":"1","pos":"2"}|{}|`, `1|{"id":"2","pos":"3"}|{}|`, `1|{"id":"3","pos":"4"}|{}|`},
		expect: "1:2 2:3 3:4"},
	{table: "bk", ddl: "create table bk(id int primary key, room int, during int4range, exclude using gist (room with =, during with &&)); " +
		"insert into bk values (1,1,'[1,2)'),(2,1,'[2,3)')",
		keys: []string{"id"}, cols: [][2]string{{"id", "int4"}, {"room", "int4"}, {"during", "int4range"}},
		raw: []string{`1|{"id":"1","room":"1","during":"[10,11)"}|{}|`, `1|{"id":"2","room":"1","during":"[1,2)"}|{}|`,
			`1|{"id":"1","room":"1","during":"[2,3)"}|{}|`},
		expect: "1:1:[2,3) 2:1:[1,2)"},
	{table: "pm", ddl: "create table pm(id int primary key, big text not null, s int); insert into pm values (1, 'BIG', 1), (2, 'y', 2)",
		keys: []string{"id"}, cols: [][2]string{{"id", "int4"}, {"big", "text"}, {"s", "int4"}},
		raw:    []string{`1|{"id":"1001","s":"5"}|{"id":"1","big":null,"s":null}|big`, `1|{"id":"2","big":"z","s":"7"}|{}|`},
		expect: "1001:BIG:5 2:z:7"},
	{table: "pk", ddl: "create table pk(id int primary key, name text unique); insert into pk values (1,'a'),(2,'b')",
		keys: []string{"id"}, cols: [][2]string{{"id", "int4"}, {"name", "text"}},
		raw: []string{`1|{"id":"10","name":"a"}|{"id":"1","name":null}|`, `1|{"id":"1","name":"b"}|{"id":"2","name":null}|`,
			`2|{"id":"10","name":null}|{"id":"10","name":null}|`},
		expect: "1:b"},
	{table: "np", ddl: "create table np(a int, b text, f float8); insert into np values (1,'x',1),(1,'x',1),(2,null,2),(3,'z','NaN')",
		full: true, cols: [][2]string{{"a", "int4"}, {"b", "text"}, {"f", "float8"}},
		raw: []string{`2|{"a":"1","b":"x","f":"1"}|{"a":"1","b":"x","f":"1"}|`,
			`1|{"a":"2","b":"y","f":"2"}|{"a":"2","b":null,"f":"2"}|`, `1|{"a":"3","b":"z","f":"-3"}|{"a":"3","b":"z","f":"NaN"}|`,
			`0|{"a":"4","b":"w","f":"4"}|{}|`, `0|{"a":"4","b":"w","f":"4"}|{}|`},
		expect: "1:x:1 2:y:2 3:z:-3 4:w:4 4:w:4"},
	{table: "nj", ddl: "create table nj(j json, p point, x xml); insert into nj values ('{\"a\": 1}', '(1,2)', '<a/>'), ('{\"a\": 1}', '(1,2)', '<a/>')",
		full: true, cols: [][2]string{{"j", "json"}, {"p", "point"}, {"x", "xml"}},
		raw: []string{`1|{"j":"{\"a\": 2}","p":"(1,2)","x":"<a/>"}|{"j":"{\"a\": 1}","p":"(1,2)","x":"<a/>"}|`,
			`2|{"j":"{\"a\": 1}","p":"(1,2)","x":"<a/>"}|{"j":"{\"a\": 1}","p":"(1,2)","x":"<a/>"}|`},
		expect: `{"a": 2}:(1,2):<a/>`},
	{table: "ri", ddl: "create table ri(id int primary key, u int not null unique, v text); insert into ri values (1,10,'a'),(2,20,'b'),(3,30,'c')",
		keys: []string{"id"}, cols: [][2]string{{"id", "int4"}, {"u", "int4"}, {"v", "text"}},
		raw: []string{`2|{"id":null,"u":"20","v":null}|{"id":null,"u":"20","v":null}|`,
			`1|{"id":"3","u":"31","v":"c"}|{"id":null,"u":"30","v":null}|`},
		expect: "1:10:a 3:31:c"},
	{table: "cmp", ddl: "create type cmp_t as (a int, b text); create table cmp(id int primary key, c cmp_t, cs cmp_t[], big text not null)",
		keys: []string{"id"}, cols: [][2]string{{"id", "int4"}, {"c", "cmp_t"}, {"cs", "_cmp_t"}, {"big", "text"}},
		raw:    []string{`0|{"id":"1","c":"(1,x)","cs":"{\"(2,y)\"}","big":"B"}|{}|`, `1|{"id":"7","c":"(3,z)","cs":"{}"}|{"id":"1","c":null,"cs":null,"big":null}|big`},
		expect: `7:(3,z):{}:B`},
	{table: "ida", ddl: "create table ida(id int generated always as identity primary key, n int generated always as identity (maxvalue 5 cycle) unique, v text)",
		keys: []string{"id"}, cols: [][2]string{{"id", "int4"}, {"n", "int4"}, {"v", "text"}},
		raw:    []string{`0|{"id":"1000","n":"3","v":"x"}|{}|`, `1|{"id":"1000","n":"3","v":"y"}|{}|`},
		expect: "1000:3:y"},
	{table: "nm", ddl: "create table nm(id int primary key, _peerdb_record_type int, _peerdb_rank int, r int)",
		keys: []string{"id"}, cols: [][2]string{{"id", "int4"}, {"_peerdb_record_type", "int4"}, {"_peerdb_rank", "int4"}, {"r", "int4"}},
		raw:    []string{`0|{"id":"1","_peerdb_record_type":"7","_peerdb_rank":"8","r":"9"}|{}|`, `0|{"id":"2","_peerdb_record_type":"7","_peerdb_rank":"8","r":"9"}|{}|`, `2|{"id":"2","_peerdb_record_type":null,"_peerdb_rank":null,"r":null}|{"id":"2"}|`},
		expect: "1:7:8:9"},
	{table: "sd", ddl: "create table sd(id int primary key, v text, del bool, at timestamptz); insert into sd values (1,'a',false,null),(2,'b',false,null)",
		soft: true, keys: []string{"id"}, cols: [][2]string{{"id", "int4"}, {"v", "text"}},
		raw:    []string{`1|{"id":"10","v":"a"}|{"id":"1","v":null}|`, `2|{"id":"2","v":null}|{"id":"2","v":null}|`, `2|{"id":"3","v":null}|{"id":"3","v":null}|`},
		expect: "10:a 2:b 3:"},
	{table: "hot", ddl: "create table hot(id int primary key, v int); insert into hot values (1,0),(2,0)",
		keys: []string{"id"}, cols: [][2]string{{"id", "int4"}, {"v", "int4"}},
		raw: []string{`1|{"id":"1","v":"1"}|{}|`, `1|{"id":"1","v":"2"}|{}|`, `2|{"id":"2","v":null}|{"id":"2","v":null}|`,
			`0|{"id":"2","v":"5"}|{}|`, `0|{"id":"3","v":"6"}|{}|`},
		expect: "1:2 2:5 3:6"},
}

func TestNormalizeRecords(t *testing.T) {
	ctx := t.Context()
	c, err := NewPostgresConnector(ctx, nil, internal.GetCatalogPostgresConfigFromEnv(ctx))
	require.NoError(t, err)
	defer c.Close()
	_, err = c.conn.Exec(ctx, "create extension if not exists btree_gist")
	require.NoError(t, err)
	require.NoError(t, c.SetupMetadataTables(ctx))
	for _, ac := range applyCases {
		t.Run(ac.table, func(t *testing.T) {
			job := "apply_" + strings.ToLower(common.RandomString(8))
			name := job + "." + ac.table
			_, err := c.CreateRawTable(ctx, &protos.CreateRawTableInput{FlowJobName: job})
			require.NoError(t, err)
			_, err = c.conn.Exec(ctx, fmt.Sprintf("insert into %s.%s values ($1, 0, 1, 0)", c.metadataSchema, mirrorJobsTableIdentifier), job)
			require.NoError(t, err)
			_, err = c.conn.Exec(ctx, fmt.Sprintf("create schema %s; set search_path = %s, public; %s", job, job, ac.ddl))
			require.NoError(t, err)
			for i, r := range ac.raw {
				f := strings.SplitN(r, "|", 4)
				_, err := c.conn.Exec(ctx, fmt.Sprintf(`insert into %s.%s values (gen_random_uuid(), $1, $2, $3, $4::text::int, $5, 1, $6)`,
					c.metadataSchema, getRawTableIdentifier(job)), i+1, name, f[1], f[0], f[2], f[3])
				require.NoError(t, err)
			}
			req := &model.NormalizeRecordsRequest{FlowJobName: job, SyncBatchID: 1,
				TableNameSchemaMapping: map[string]*protos.TableSchema{name: applySchema(ac)}}
			if ac.soft {
				req.SoftDeleteColName, req.SyncedAtColName = "del", "at"
			}
			_, err = c.NormalizeRecords(ctx, req)
			require.NoError(t, err)
			if ac.soft {
				var deleted string
				require.NoError(t, c.conn.QueryRow(ctx, fmt.Sprintf("select string_agg(id || ':' || del || ':' || (at is not null), ' ' order by id) from %s", name)).Scan(&deleted))
				require.Equal(t, "2:true:true 3:true:true 10:false:true", deleted)
			}
			cols := make([]string, 0, len(ac.cols))
			for _, col := range ac.cols {
				cols = append(cols, fmt.Sprintf("coalesce(%s::text, '')", col[0]))
			}
			row := strings.Join(cols, "||':'||")
			var got string
			require.NoError(t, c.conn.QueryRow(ctx, fmt.Sprintf("select coalesce(string_agg(%s, ' ' order by %s), '') from %s",
				row, row, name)).Scan(&got))
			require.Equal(t, ac.expect, got)
		})
	}
}

func TestNormalizeThroughput(t *testing.T) {
	rows, _ := strconv.Atoi(os.Getenv("PEERDB_APPLY_BENCH_ROWS"))
	if rows == 0 {
		t.Skip("set PEERDB_APPLY_BENCH_ROWS")
	}
	ctx := t.Context()
	c, err := NewPostgresConnector(ctx, nil, internal.GetCatalogPostgresConfigFromEnv(ctx))
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.SetupMetadataTables(ctx))
	for _, shape := range []string{"update", "insert", "pkmove"} {
		job := "bench_" + strings.ToLower(common.RandomString(8))
		name := job + ".t"
		_, err := c.CreateRawTable(ctx, &protos.CreateRawTableInput{FlowJobName: job})
		require.NoError(t, err)
		_, err = c.conn.Exec(ctx, fmt.Sprintf("insert into %s.%s values ($1, 0, 1, 0)", c.metadataSchema, mirrorJobsTableIdentifier), job)
		require.NoError(t, err)
		_, err = c.conn.Exec(ctx, fmt.Sprintf(`create schema %[1]s; create table %[2]s(id bigint primary key, a int not null, b text, u bigint unique);
			insert into %[2]s select g, g, md5(g::text), g from generate_series(1, %[3]d) g`, job, name, rows))
		require.NoError(t, err)
		key, typ, match := "g", 1, "'{}'::jsonb"
		switch shape {
		case "insert":
			key, typ = "g + $2::int", 0
		case "pkmove":
			key, match = "case when g = 1 then -1 else g end", "case when g = 1 then '{\"id\":\"1\"}'::jsonb else '{}' end"
		}
		_, err = c.conn.Exec(ctx, fmt.Sprintf(`insert into %s.%s select gen_random_uuid(), g, $1,
			jsonb_build_object('id', (%s)::text, 'a', (g + 1)::text, 'b', md5(g::text), 'u', (%s)::text), %d, %s, 1, ''
			from generate_series(1, $2::int) g`, c.metadataSchema, getRawTableIdentifier(job), key, key, typ, match), name, rows)
		require.NoError(t, err)
		_, err = c.conn.Exec(ctx, fmt.Sprintf("vacuum analyze %s.%s", c.metadataSchema, getRawTableIdentifier(job)))
		require.NoError(t, err)
		start := time.Now()
		_, err = c.NormalizeRecords(ctx, &model.NormalizeRecordsRequest{FlowJobName: job, SyncBatchID: 1,
			TableNameSchemaMapping: map[string]*protos.TableSchema{name: {TableIdentifier: name, PrimaryKeyColumns: []string{"id"},
				System: protos.TypeSystem_PG, Columns: []*protos.FieldDescription{{Name: "id", Type: "int8"}, {Name: "a", Type: "int4"},
					{Name: "b", Type: "text"}, {Name: "u", Type: "int8"}}}}})
		require.NoError(t, err)
		t.Logf("BENCH shape=%s rows=%d normalize_s=%.2f", shape, rows, time.Since(start).Seconds())
	}
}
