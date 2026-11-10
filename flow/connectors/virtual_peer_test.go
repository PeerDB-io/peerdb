package connectors

import "testing"

func TestSplitVirtualPeerName(t *testing.T) {
	cases := []struct {
		name, base, database string
		ok                   bool
	}{
		{"analytics__peerdb_pruebas", "analytics", "peerdb_pruebas", true},
		{"analytics__my__db", "analytics", "my__db", true},
		{"analytics_peerdb_pruebas", "", "", false},
		{"analytics__", "", "", false},
		{"__peerdb_pruebas", "", "", false},
		{"analytics__Bad", "", "", false},
		{"analytics__1db", "", "", false},
		{"analytics", "", "", false},
	}
	for _, c := range cases {
		base, database, ok := SplitVirtualPeerName(c.name)
		if base != c.base || database != c.database || ok != c.ok {
			t.Errorf("SplitVirtualPeerName(%q) = (%q, %q, %v), want (%q, %q, %v)",
				c.name, base, database, ok, c.base, c.database, c.ok)
		}
	}
}
