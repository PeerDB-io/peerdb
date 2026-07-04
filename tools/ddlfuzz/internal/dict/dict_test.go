package dict

import "testing"

func TestDictionaryFiles(t *testing.T) {
	for _, engine := range []string{"mysql", "mariadb"} {
		toks := Tokens(engine)
		min := 500
		if engine == "mariadb" {
			min = 600
		}
		if len(toks) <= min {
			t.Fatalf("%s dictionary too small: %d", engine, len(toks))
		}
		for _, tok := range toks {
			for i := 0; i < len(tok); i++ {
				if tok[i] >= 0x80 {
					t.Fatalf("%s token is not ASCII: %q", engine, tok)
				}
			}
		}
	}
}

func TestDictionaryCampaignEntries(t *testing.T) {
	for _, engine := range []string{"mysql", "mariadb"} {
		have := map[string]struct{}{}
		for _, tok := range Tokens(engine) {
			have[tok] = struct{}{}
			if tok == "peerdb_ddlfuzz_nodb" {
				t.Fatalf("%s dictionary emits sentinel", engine)
			}
		}
		for _, want := range []string{"/*", "*/", "/*!", "/*!99999", "-- ", "#", "'", "`", "\"", ";", "REMOVE PARTITIONING", "WAIT 3", "NOWAIT", "IF NOT EXISTS"} {
			if _, ok := have[want]; !ok {
				t.Fatalf("%s missing %q", engine, want)
			}
		}
	}
	maria := map[string]struct{}{}
	for _, tok := range Tokens("mariadb") {
		maria[tok] = struct{}{}
	}
	if _, ok := maria["/*M!"]; !ok {
		t.Fatal("mariadb missing /*M!")
	}
}
