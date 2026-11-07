//go:build tilt

package connpostgres

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

// routingFixture builds inheritance hierarchies in a schema of its own and checks how CDC routes their relations.
type routingFixture struct {
	t      *testing.T
	c      *PostgresConnector
	schema string
	names  map[uint32]string
}

func newRoutingFixture(t *testing.T, c *PostgresConnector) *routingFixture {
	t.Helper()
	return &routingFixture{t: t, c: c, schema: createSchema(t, c.conn), names: map[uint32]string{}}
}

// create makes table name, inheriting from parents in order, so the first parent gets inhseqno 1.
func (f *routingFixture) create(name string, parents ...string) {
	f.t.Helper()
	sql := fmt.Sprintf("CREATE TABLE %s.%s (id INT)", f.schema, name)
	if len(parents) > 0 {
		qualified := make([]string, 0, len(parents))
		for _, parent := range parents {
			qualified = append(qualified, f.schema+"."+parent)
		}
		sql += " INHERITS (" + strings.Join(qualified, ", ") + ")"
	}
	f.exec(sql)
	f.track(name)
}

func (f *routingFixture) exec(sql string) {
	f.t.Helper()
	_, err := f.c.conn.Exec(f.t.Context(), sql)
	require.NoError(f.t, err, sql)
}

func (f *routingFixture) track(name string) {
	f.t.Helper()
	f.names[f.oid(name)] = name
}

func (f *routingFixture) oid(name string) uint32 {
	f.t.Helper()
	var oid uint32
	require.NoError(f.t, f.c.conn.QueryRow(f.t.Context(), "SELECT $1::regclass::oid", f.schema+"."+name).Scan(&oid))
	return oid
}

// requireRoutes checks that the startup map routes exactly the relations in expected, to the given mirrored
// tables, and that runtime discovery routes every relation that is not mirrored the same way.
func (f *routingFixture) requireRoutes(mirrored []string, handleInheritance bool, expected map[string]string) {
	f.t.Helper()
	ctx := f.t.Context()
	mirroredOIDs := make([]uint32, 0, len(mirrored))
	isMirrored := map[string]bool{}
	for _, name := range mirrored {
		mirroredOIDs = append(mirroredOIDs, f.oid(name))
		isMirrored[name] = true
	}

	startup, relkinds, err := getChildToParentRelIDMap(ctx, f.c.conn, mirroredOIDs, handleInheritance)
	require.NoError(f.t, err)
	got := map[string]string{}
	for child, parent := range startup {
		got[f.names[child]] = f.names[parent]
		require.Equal(f.t, f.relkind(f.names[parent]), relkinds[parent], "relkind of %s", f.names[parent])
	}
	require.Equal(f.t, expected, got, "startup map")

	for oid, name := range f.names {
		if isMirrored[name] {
			continue // runtime discovery only runs for relations that are not mirrored
		}
		route, found, err := findMirroredAncestor(ctx, f.c.conn, oid, mirroredOIDs, handleInheritance)
		require.NoError(f.t, err, "runtime route of %s", name)
		if want, ok := expected[name]; ok {
			require.True(f.t, found, "runtime route of %s", name)
			require.Equal(f.t, want, f.names[route.ancestor], "runtime route of %s", name)
			require.Equal(f.t, f.relkind(want), route.relkind, "runtime relkind of %s", name)
		} else {
			require.False(f.t, found, "%s has no route", name)
		}
	}
}

func (f *routingFixture) relkind(name string) byte {
	f.t.Helper()
	var relkind byte
	require.NoError(f.t, f.c.conn.QueryRow(f.t.Context(),
		"SELECT relkind FROM pg_class WHERE oid = $1::regclass", f.schema+"."+name).Scan(&relkind))
	return relkind
}

func (f *routingFixture) tables(names ...string) []*common.QualifiedTable {
	tables := make([]*common.QualifiedTable, 0, len(names))
	for _, name := range names {
		tables = append(tables, &common.QualifiedTable{Namespace: f.schema, Table: name})
	}
	return tables
}

func TestInheritanceRouting(t *testing.T) {
	t.Parallel()
	c, err := NewPostgresConnector(t.Context(), nil, internal.GetCatalogPostgresConfigFromEnv(t.Context()))
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })

	t.Run("descendants at any depth go to the mirrored root", func(t *testing.T) {
		f := newRoutingFixture(t, c)
		f.create("r")
		f.create("c", "r")
		f.create("g", "c")
		f.requireRoutes([]string{"r"}, true, map[string]string{"c": "r", "g": "r"})
	})

	t.Run("a mirrored relation keeps its own destination below an unmirrored intermediate", func(t *testing.T) {
		f := newRoutingFixture(t, c)
		f.create("r")
		f.create("u", "r")
		f.create("m", "u")
		f.create("l", "m")
		f.requireRoutes([]string{"r", "m"}, true, map[string]string{"u": "r", "l": "m"})
	})

	t.Run("a mirrored child of a mirrored parent is remapped as before, its children go to it", func(t *testing.T) {
		f := newRoutingFixture(t, c)
		f.create("p")
		f.create("q", "p")
		f.create("z", "q")
		f.requireRoutes([]string{"p", "q"}, true, map[string]string{"q": "p", "z": "q"})
	})

	t.Run("ties go to the lowest depth, then the lowest inhseqno path", func(t *testing.T) {
		f := newRoutingFixture(t, c)
		f.create("a")
		f.create("b")
		f.create("u", "a", "b")
		// paths [1,1] to a and [1,2] to b differ only at the second edge
		f.create("d", "u")
		f.create("x", "b")
		f.create("y", "a")
		// paths [1,1] to b and [2,1] to a: the first edge decides, although a was created first
		f.create("e", "x", "y")
		// a direct mirrored parent beats a nearer-numbered path at depth two
		f.create("w", "y", "b")
		f.requireRoutes([]string{"a", "b"}, true, map[string]string{
			"u": "a", "d": "a", "x": "b", "y": "a", "e": "b", "w": "b",
		})
	})

	t.Run("a diamond is routed once", func(t *testing.T) {
		f := newRoutingFixture(t, c)
		f.create("m")
		f.create("u1", "m")
		f.create("u2", "m")
		f.create("dd", "u1", "u2")
		f.requireRoutes([]string{"m"}, true, map[string]string{"u1": "m", "u2": "m", "dd": "m"})
	})

	t.Run("plain tables are not traversed with inheritance handling off", func(t *testing.T) {
		f := newRoutingFixture(t, c)
		f.create("r")
		f.create("c", "r")
		f.create("g", "c")
		f.requireRoutes([]string{"r"}, false, map[string]string{})
	})

	t.Run("multi-level partitions", func(t *testing.T) {
		f := newRoutingFixture(t, c)
		f.exec(fmt.Sprintf("CREATE TABLE %s.root (id INT, region TEXT) PARTITION BY LIST (region)", f.schema))
		f.exec(fmt.Sprintf("CREATE TABLE %[1]s.mid PARTITION OF %[1]s.root FOR VALUES IN ('a') PARTITION BY RANGE (id)", f.schema))
		f.exec(fmt.Sprintf("CREATE TABLE %[1]s.leaf PARTITION OF %[1]s.mid FOR VALUES FROM (0) TO (100)", f.schema))
		f.exec(fmt.Sprintf("CREATE TABLE %[1]s.leaf2 PARTITION OF %[1]s.root FOR VALUES IN ('b')", f.schema))
		for _, name := range []string{"root", "mid", "leaf", "leaf2"} {
			f.track(name)
		}
		all := map[string]string{"mid": "root", "leaf": "root", "leaf2": "root"}
		f.requireRoutes([]string{"root"}, false, all)
		f.requireRoutes([]string{"root"}, true, all)
		// a mirrored partitioned subtree below an unmirrored root
		f.requireRoutes([]string{"mid"}, false, map[string]string{"leaf": "mid"})
	})

	t.Run("chained diamonds are walked once per relation, not once per path", func(t *testing.T) {
		f := newRoutingFixture(t, c)
		f.create("n0")
		expected := map[string]string{}
		// the last relation has 2^25 inheritance paths to n0
		for i := 1; i <= 25; i++ {
			prev := fmt.Sprintf("n%d", i-1)
			a, b, n := fmt.Sprintf("a%d", i), fmt.Sprintf("b%d", i), fmt.Sprintf("n%d", i)
			f.create(a, prev)
			f.create(b, prev)
			f.create(n, a, b)
			expected[a], expected[b], expected[n] = "n0", "n0", "n0"
		}
		f.requireRoutes([]string{"n0"}, true, expected)
	})

	t.Run("hierarchies deeper than 32 levels", func(t *testing.T) {
		f := newRoutingFixture(t, c)
		f.create("t0")
		expected := map[string]string{}
		for i := 1; i <= 40; i++ {
			name := fmt.Sprintf("t%d", i)
			f.create(name, fmt.Sprintf("t%d", i-1))
			expected[name] = "t0"
		}
		f.requireRoutes([]string{"t0"}, true, expected)
	})
}

func TestOverlappingInheritanceWarnings(t *testing.T) {
	t.Parallel()
	c, err := NewPostgresConnector(t.Context(), nil, internal.GetCatalogPostgresConfigFromEnv(t.Context()))
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })
	handling := func(on bool) map[string]string {
		return map[string]string{"PEERDB_POSTGRES_CDC_HANDLE_INHERITANCE_FOR_NON_PARTITIONED_TABLES": strconv.FormatBool(on)}
	}
	messages := func(warnings []error) []string {
		out := make([]string, 0, len(warnings))
		for _, warning := range warnings {
			out = append(out, warning.Error())
		}
		return out
	}

	t.Run("each overlapping relation names the mapping that receives its changes", func(t *testing.T) {
		f := newRoutingFixture(t, c)
		f.create("m1")
		f.create("m2", "m1")
		f.create("g", "m2")
		f.create("solo")
		warnings, err := c.overlappingInheritanceWarnings(t.Context(), handling(true), f.tables("m1", "m2", "solo"), nil)
		require.NoError(t, err)
		s := f.schema
		require.Equal(t, []string{
			fmt.Sprintf("rows of %[1]s.g are included in the snapshot scope of %[1]s.m1, %[1]s.m2; "+
				"its changes are replicated through the mapping of %[1]s.m2 only", s),
			fmt.Sprintf("rows of %[1]s.m2 are included in the snapshot scope of %[1]s.m1, %[1]s.m2; "+
				"its changes are replicated through the mapping of %[1]s.m1 only", s),
		}, messages(warnings))
	})

	t.Run("without a route the changes are not replicated", func(t *testing.T) {
		f := newRoutingFixture(t, c)
		f.create("p")
		f.create("m", "p")
		f.create("x", "m")
		// neither m nor x is remapped: plain parents do not qualify with inheritance handling off
		f.requireRoutes([]string{"p", "m"}, false, map[string]string{})
		warnings, err := c.overlappingInheritanceWarnings(t.Context(), handling(false), f.tables("p", "m"), nil)
		require.NoError(t, err)
		s := f.schema
		require.Equal(t, []string{
			fmt.Sprintf("rows of %[1]s.m are included in the snapshot scope of %[1]s.m, %[1]s.p; "+
				"its changes are replicated through the mapping of %[1]s.m only", s),
			fmt.Sprintf("rows of %[1]s.x are included in the snapshot scope of %[1]s.m, %[1]s.p; "+
				"its changes are not replicated by CDC", s),
		}, messages(warnings))
	})

	t.Run("overlaps through a relation with two parents", func(t *testing.T) {
		f := newRoutingFixture(t, c)
		f.create("a")
		f.create("b")
		f.create("u", "a", "b")
		f.create("x", "u")
		warnings, err := c.overlappingInheritanceWarnings(t.Context(), handling(true), f.tables("a", "b"), nil)
		require.NoError(t, err)
		s := f.schema
		require.Equal(t, []string{
			fmt.Sprintf("rows of 2 relations (%[1]s.u, %[1]s.x) are included in the snapshot scope of %[1]s.a, %[1]s.b; "+
				"their changes are replicated through the mapping of %[1]s.a only", s),
		}, messages(warnings))
	})

	t.Run("relations with the same owners and route share a warning", func(t *testing.T) {
		f := newRoutingFixture(t, c)
		f.create("m1")
		f.create("m2", "m1")
		for i := range 5 {
			f.create(fmt.Sprintf("c%d", i), "m2")
		}
		warnings, err := c.overlappingInheritanceWarnings(t.Context(), handling(true), f.tables("m1", "m2"), nil)
		require.NoError(t, err)
		s := f.schema
		require.Equal(t, []string{
			fmt.Sprintf("rows of 5 relations (%[1]s.c0, %[1]s.c1, %[1]s.c2 and 2 more) are included in the snapshot "+
				"scope of %[1]s.m1, %[1]s.m2; their changes are replicated through the mapping of %[1]s.m2 only", s),
			fmt.Sprintf("rows of %[1]s.m2 are included in the snapshot scope of %[1]s.m1, %[1]s.m2; "+
				"its changes are replicated through the mapping of %[1]s.m1 only", s),
		}, messages(warnings))
	})

	t.Run("no overlap below a single mirrored table", func(t *testing.T) {
		f := newRoutingFixture(t, c)
		f.create("m")
		f.create("c", "m")
		// a relation with a second, unmirrored parent
		f.create("q")
		f.create("r")
		f.create("z", "q", "r")
		// a mirrored table under an unmirrored one
		f.create("p")
		f.create("n", "p")
		f.create("l", "n")
		warnings, err := c.overlappingInheritanceWarnings(t.Context(), handling(true), f.tables("m", "q", "n"), nil)
		require.NoError(t, err)
		require.Empty(t, warnings)
	})

	t.Run("table additions report only overlaps an added table takes part in", func(t *testing.T) {
		f := newRoutingFixture(t, c)
		f.create("m1")
		f.create("m2", "m1")
		f.create("g", "m2")
		f.create("other")
		warnings, err := c.overlappingInheritanceWarnings(t.Context(), handling(true),
			f.tables("m1", "m2"), f.tables("m2"))
		require.NoError(t, err)
		require.Len(t, warnings, 2, "the added table overlaps an existing mapping")
		warnings, err = c.overlappingInheritanceWarnings(t.Context(), handling(true),
			f.tables("m1", "m2", "other"), f.tables("other"))
		require.NoError(t, err)
		require.Empty(t, warnings, "existing overlaps are reported at setup, not again")
	})
}
