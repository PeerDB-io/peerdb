package connpostgres

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

var updateGolden = flag.Bool("update", false, "rewrite the golden files in testdata/ordered")

// The DO blocks in testdata/ordered/*.sql are the reviewable form of generateOrderedStatement:
// read those to see what a given table shape replays. Regenerate after changing the generator with
//
//	go test ./connectors/postgres/ -run TestGenerateOrderedStatement -update
func TestGenerateOrderedStatement(t *testing.T) {
	col := func(name, typ string) *protos.FieldDescription {
		return &protos.FieldDescription{Name: name, Type: typ, TypeModifier: -1}
	}
	cases := []struct {
		name    string
		schema  *protos.TableSchema
		toast   []string
		dst     destinationTableIndexInfo
		cols    *protos.PeerDBColumns
		alwaysI []string
	}{
		{
			// keyed replay: match the old key, keep unchanged TOAST, upsert on the new key
			name: "keyed_unchanged_toast",
			schema: &protos.TableSchema{
				PrimaryKeyColumns: []string{"id"},
				Columns:           []*protos.FieldDescription{col("id", "int4"), col("c", "int4"), col("big", "text")},
			},
			toast: []string{"", "big"},
			dst:   destinationTableIndexInfo{isPrimaryKeyUnique: true},
		},
		{
			// whole-row replay: replica identity full without a unique key on the destination
			name: "whole_row_replica_identity_full",
			schema: &protos.TableSchema{
				PrimaryKeyColumns:     []string{"id"},
				IsReplicaIdentityFull: true,
				Columns:               []*protos.FieldDescription{col("id", "int4"), col("v", "text"), col("j", "jsonb")},
			},
		},
		{
			// soft delete and synced-at columns, with an ALWAYS identity key that is never updated
			name: "soft_delete_always_identity",
			schema: &protos.TableSchema{
				PrimaryKeyColumns: []string{"id"},
				Columns:           []*protos.FieldDescription{col("id", "int8"), col("v", "text")},
			},
			dst:     destinationTableIndexInfo{isPrimaryKeyUnique: true},
			cols:    &protos.PeerDBColumns{SoftDeleteColName: "_PEERDB_IS_DELETED", SyncedAtColName: "_PEERDB_SYNCED_AT"},
			alwaysI: []string{"id"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			const table = "public.t"
			tc.schema.TableIdentifier, tc.schema.System = table, protos.TypeSystem_PG
			if tc.cols == nil {
				tc.cols = &protos.PeerDBColumns{}
			}
			gen := normalizeStmtGenerator{
				rawTableName:             "_peerdb_raw_mirror",
				metadataSchema:           "_peerdb_internal",
				peerdbCols:               tc.cols,
				tableSchemaMapping:       map[string]*protos.TableSchema{table: tc.schema},
				unchangedToastColumnsMap: map[string][]string{table: tc.toast},
				alwaysIdentityColumns:    map[string][]string{table: tc.alwaysI},
				dst:                      map[string]destinationTableIndexInfo{table: tc.dst},
			}
			stmt := gen.generateOrderedStatement(table, 7)
			got := formatOrderedStatement(stmt)
			require.Equal(t, strings.Join(strings.Fields(stmt), " "), strings.Join(strings.Fields(got), " "),
				"formatting must only change whitespace")

			path := filepath.Join("testdata", "ordered", tc.name+".sql")
			if *updateGolden {
				require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
				require.NoError(t, os.WriteFile(path, []byte(got), 0o600))
			}
			want, err := os.ReadFile(path)
			require.NoError(t, err, "run with -update to create the golden file")
			require.Equal(t, string(want), got)
		})
	}
}

// formatOrderedStatement puts each statement of the loop body on its own line and indents IF/ELSE/END IF,
// changing only whitespace. The IF conditions in the body never contain THEN, so the first THEN ends one.
func formatOrderedStatement(stmt string) string {
	head, body, ok := strings.Cut(stmt, "\nLOOP ")
	if !ok {
		return stmt
	}
	body, tail, _ := strings.Cut(body, " END LOOP;")
	var b strings.Builder
	b.WriteString(head + "\nLOOP\n")
	depth := 1
	line := func(s string) { b.WriteString(strings.Repeat("  ", depth) + s + "\n") }
	for _, seg := range strings.SplitAfter(body, "; ") {
		seg = strings.TrimSpace(seg)
		for seg != "" {
			switch {
			case seg == "END IF;":
				depth--
				line(seg)
				seg = ""
			case strings.HasPrefix(seg, "ELSE "):
				depth--
				line("ELSE")
				depth++
				seg = seg[len("ELSE "):]
			case strings.HasPrefix(seg, "IF "):
				cond, rest, _ := strings.Cut(seg, " THEN ")
				line(cond + " THEN")
				depth++
				seg = rest
			default:
				line(seg)
				seg = ""
			}
		}
	}
	b.WriteString("END LOOP;" + tail + "\n")
	return b.String()
}
