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
