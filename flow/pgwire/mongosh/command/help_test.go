package command

import (
	"strings"
	"testing"
)

func TestGlobalHelp(t *testing.T) {
	help := GlobalHelp()

	// Should contain shell methods
	if !strings.Contains(help, "Shell Methods:") {
		t.Error("expected 'Shell Methods:' header")
	}
	if !strings.Contains(help, "find") {
		t.Error("expected 'find' in shell methods")
	}

	// Should contain wire commands
	if !strings.Contains(help, "Wire Commands:") {
		t.Error("expected 'Wire Commands:' header")
	}
	if !strings.Contains(help, "ping") {
		t.Error("expected 'ping' in wire commands")
	}
}

func TestDatabaseHelp(t *testing.T) {
	help := DatabaseHelp()

	if !strings.Contains(help, "Database Methods:") {
		t.Error("expected 'Database Methods:' header")
	}
	if !strings.Contains(help, "runcommand") {
		t.Error("expected 'runcommand' in database methods")
	}
}

func TestCollectionHelp(t *testing.T) {
	help := CollectionHelp()

	if !strings.Contains(help, "Collection Methods:") {
		t.Error("expected 'Collection Methods:' header")
	}

	expected := []string{"find", "aggregate", "distinct"}
	for _, m := range expected {
		if !strings.Contains(help, m) {
			t.Errorf("expected %q in collection methods", m)
		}
	}
}

func TestMethodHelp(t *testing.T) {
	help := MethodHelp("find")

	if !strings.Contains(help, "find() chainers:") {
		t.Error("expected 'find() chainers:' header")
	}

	expected := []string{"sort", "limit", "skip"}
	for _, c := range expected {
		if !strings.Contains(help, c) {
			t.Errorf("expected chainer %q in find help", c)
		}
	}
}

func TestMethodHelp_Unknown(t *testing.T) {
	help := MethodHelp("unknown")
	if !strings.Contains(help, "Unknown method") {
		t.Error("expected 'Unknown method' for unknown method")
	}
}
