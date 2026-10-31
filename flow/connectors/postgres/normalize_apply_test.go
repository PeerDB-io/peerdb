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

type applyCase struct {
	table  string
	ddl    string
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
	{table: "ida", ddl: "create table ida(id int generated always as identity primary key, n int generated always as identity (maxvalue 5 cycle) unique, v text)",
		keys: []string{"id"}, cols: [][2]string{{"id", "int4"}, {"n", "int4"}, {"v", "text"}},
		raw:    []string{`0|{"id":"1000","n":"3","v":"x"}|{}|`, `1|{"id":"1000","n":"3","v":"y"}|{}|`},
		expect: "1000:3:y"},
}

func TestNormalizeRecords(t *testing.T) {
	ctx := t.Context()
	c, err := NewPostgresConnector(ctx, nil, internal.GetCatalogPostgresConfigFromEnv(ctx))
	require.NoError(t, err)
	defer c.Close()
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
			_, err = c.NormalizeRecords(ctx, &model.NormalizeRecordsRequest{FlowJobName: job, SyncBatchID: 1,
				TableNameSchemaMapping: map[string]*protos.TableSchema{name: applySchema(ac)}})
			require.NoError(t, err)
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
