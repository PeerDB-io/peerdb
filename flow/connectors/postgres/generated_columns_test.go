//go:build tilt

package connpostgres

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

func TestGetSelectedColumnsSkipsGenerated(t *testing.T) {
	ctx := t.Context()
	c, err := NewPostgresConnector(ctx, nil, internal.GetCatalogPostgresConfigFromEnv(ctx))
	require.NoError(t, err)
	defer c.Close()
	s := "gen_" + strings.ToLower(common.RandomString(8))
	_, err = c.conn.Exec(ctx, fmt.Sprintf(
		"create schema %[1]s; create table %[1]s.t(id int primary key, a int, b int generated always as (a * 2) stored)", s))
	require.NoError(t, err)
	table := &common.QualifiedTable{Namespace: s, Table: "t"}
	cols, err := c.GetSelectedColumns(ctx, table, nil, protos.TypeSystem_PG)
	require.NoError(t, err)
	require.Equal(t, []string{"id", "a"}, cols)
	cols, err = c.GetSelectedColumns(ctx, table, []string{"a"}, protos.TypeSystem_Q)
	require.NoError(t, err)
	require.Equal(t, []string{"id", "b"}, cols)
}

func TestCheckGeneratedKeyColumns(t *testing.T) {
	ctx := t.Context()
	c, err := NewPostgresConnector(ctx, nil, internal.GetCatalogPostgresConfigFromEnv(ctx))
	require.NoError(t, err)
	defer c.Close()
	s := "genkey_" + strings.ToLower(common.RandomString(8))
	_, err = c.conn.Exec(ctx, fmt.Sprintf(`create schema %[1]s;
		create table %[1]s.ok(id int generated always as identity primary key, a int, b int generated always as (a * 2) stored);
		create table %[1]s.genpk(a int, b int generated always as (a * 2) stored primary key);
		create table %[1]s.genri(id int primary key, a int not null, b int not null generated always as (a * 2) stored);
		create unique index genri_b on %[1]s.genri(b); alter table %[1]s.genri replica identity using index genri_b;
		create table %[1]s.riok(a int not null, b int generated always as (a * 2) stored primary key);
		create unique index riok_a on %[1]s.riok(a); alter table %[1]s.riok replica identity using index riok_a`, s))
	require.NoError(t, err)
	table := func(name string) *common.QualifiedTable { return &common.QualifiedTable{Namespace: s, Table: name} }

	require.NoError(t, c.checkGeneratedKeyColumns(ctx, []*common.QualifiedTable{table("ok"), table("riok")}))
	err = c.checkGeneratedKeyColumns(ctx, []*common.QualifiedTable{table("ok"), table("genpk"), table("genri")})
	require.ErrorContains(t, err, fmt.Sprintf(`"%[1]s"."genpk"."b", "%[1]s"."genri"."b"`, s))
}

func TestNormalizeAlwaysIdentity(t *testing.T) {
	ctx := t.Context()
	c, err := NewPostgresConnector(ctx, nil, internal.GetCatalogPostgresConfigFromEnv(ctx))
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.SetupMetadataTables(ctx))

	for _, tc := range []struct {
		name  string
		merge bool
		soft  bool
	}{
		{name: "merge", merge: true},
		{name: "merge_soft_delete", merge: true, soft: true},
		{name: "fallback"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := "ident_" + strings.ToLower(common.RandomString(8))
			dst := job + ".t"
			_, err := c.CreateRawTable(ctx, &protos.CreateRawTableInput{FlowJobName: job})
			require.NoError(t, err)
			_, err = c.conn.Exec(ctx, fmt.Sprintf("insert into %s.%s values ($1, 0, 1, 0)",
				c.metadataSchema, mirrorJobsTableIdentifier), job)
			require.NoError(t, err)
			_, err = c.conn.Exec(ctx, fmt.Sprintf(`create schema %[1]s; create table %[1]s.t(
				id int generated always as identity primary key, n int generated always as identity unique, v text,
				del bool, at timestamptz); create table %[1]s.only_id(id int generated always as identity primary key, del bool, at timestamptz)`, job))
			require.NoError(t, err)

			raw := []struct {
				table, data, match string
				typ                int
			}{
				{dst, `{"id":"1000","n":"3","v":"x"}`, `{}`, 0},
				{dst, `{"id":"1001","n":"4","v":"a"}`, `{}`, 0},
				{dst, `{"id":"1002","n":"5","v":"b"}`, `{}`, 0},
				{job + ".only_id", `{"id":"7"}`, `{}`, 0},
			}
			for i, r := range raw {
				_, err := c.conn.Exec(ctx, fmt.Sprintf(`insert into %s.%s values (gen_random_uuid(), $1, $2, $3, $4, $5, 1, '')`,
					c.metadataSchema, getRawTableIdentifier(job)), i+1, r.table, r.data, r.typ, r.match)
				require.NoError(t, err)
			}
			raw = []struct {
				table, data, match string
				typ                int
			}{
				{dst, `{"id":"1000","n":"3","v":"y"}`, `{}`, 1},
				{dst, `{"id":"1001","n":"4","v":"a"}`, `{"id":"1001","n":null,"v":null}`, 2},
				{job + ".only_id", `{"id":"7"}`, `{}`, 1},
				{job + ".only_id", `{"id":"8"}`, `{}`, 0},
			}
			for i, r := range raw {
				_, err := c.conn.Exec(ctx, fmt.Sprintf(`insert into %s.%s values (gen_random_uuid(), $1, $2, $3, $4, $5, 2, '')`,
					c.metadataSchema, getRawTableIdentifier(job)), 100+i, r.table, r.data, r.typ, r.match)
				require.NoError(t, err)
			}

			schemas := map[string]*protos.TableSchema{
				dst: {
					TableIdentifier: dst, PrimaryKeyColumns: []string{"id"}, System: protos.TypeSystem_PG,
					Columns: []*protos.FieldDescription{
						{Name: "id", Type: "int4", TypeModifier: -1},
						{Name: "n", Type: "int4", TypeModifier: -1},
						{Name: "v", Type: "text", TypeModifier: -1},
					},
				},
				job + ".only_id": {
					TableIdentifier: job + ".only_id", PrimaryKeyColumns: []string{"id"}, System: protos.TypeSystem_PG,
					Columns: []*protos.FieldDescription{{Name: "id", Type: "int4", TypeModifier: -1}},
				},
			}
			req := &model.NormalizeRecordsRequest{FlowJobName: job, SyncBatchID: 2, TableNameSchemaMapping: schemas}
			if tc.soft {
				req.SoftDeleteColName, req.SyncedAtColName = "del", "at"
			}

			if tc.merge {
				_, err = c.NormalizeRecords(ctx, req)
				require.NoError(t, err)
			} else {
				identity, err := c.getAlwaysIdentityColumns(ctx, []string{dst, job + ".only_id"})
				require.NoError(t, err)
				require.Equal(t, map[string][]string{dst: {"id", "n"}, job + ".only_id": {"id"}}, identity)
				gen := normalizeStmtGenerator{
					Logger: c.logger, rawTableName: getRawTableIdentifier(job), tableSchemaMapping: schemas,
					peerdbCols: &protos.PeerDBColumns{}, metadataSchema: c.metadataSchema, alwaysIdentityColumns: identity,
				}
				for batchID := int64(1); batchID <= 2; batchID++ {
					for _, table := range []string{dst, job + ".only_id"} {
						for _, stmt := range gen.generateNormalizeStatements(table) {
							_, err := c.conn.Exec(ctx, stmt, batchID, table)
							require.NoError(t, err)
						}
					}
				}
			}

			expected := "1000:3:y 1002:5:b"
			if tc.soft {
				expected = "1000:3:y:false 1001:4:a:true 1002:5:b:"
			}
			cols := "id || ':' || n || ':' || v"
			if tc.soft {
				cols += " || ':' || coalesce(del::text, '')"
			}
			var got string
			require.NoError(t, c.conn.QueryRow(ctx,
				fmt.Sprintf("select string_agg(%s, ' ' order by id) from %s", cols, dst)).Scan(&got))
			require.Equal(t, expected, got)
			require.NoError(t, c.conn.QueryRow(ctx,
				fmt.Sprintf("select string_agg(id::text, ' ' order by id) from %s.only_id", job)).Scan(&got))
			require.Equal(t, "7 8", got)
		})
	}
}
