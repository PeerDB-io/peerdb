package command

import (
	"fmt"
	"strings"
	"testing"
)

func flattenRows(rows [][]string) string {
	var parts []string
	for _, row := range rows {
		parts = append(parts, strings.Join(row, " "))
	}
	return strings.Join(parts, "\n")
}

func TestGlobalHelp(t *testing.T) {
	cols, rows := GlobalHelp()
	if cols[0] != "help" {
		t.Errorf("expected help column, got %v", cols)
	}
	help := flattenRows(rows)
	for _, want := range []string{"Shell Methods:", "Wire Commands:", "find", "ping", "helpCommands()"} {
		if !strings.Contains(help, want) {
			t.Errorf("missing %q in global help", want)
		}
	}
}

func TestWireCommandHelp(t *testing.T) {
	cols, rows := WireCommandHelp()
	if cols[0] != "command" || cols[1] != "docs" {
		t.Errorf("expected [command docs], got %v", cols)
	}
	help := flattenRows(rows)
	for _, want := range []string{"ping", "find", "/reference/command/"} {
		if !strings.Contains(help, want) {
			t.Errorf("missing %q in wire command help", want)
		}
	}
}

func TestDatabaseHelp(t *testing.T) {
	cols, rows := DatabaseHelp()
	if cols[0] != "method" || cols[1] != "docs" {
		t.Errorf("expected [method docs], got %v", cols)
	}
	help := flattenRows(rows)
	for _, want := range []string{"runCommand", "/method/db.runCommand/"} {
		if !strings.Contains(help, want) {
			t.Errorf("missing %q in database help", want)
		}
	}
}

func TestCollectionHelp(t *testing.T) {
	cols, rows := CollectionHelp()
	if cols[0] != "method" || cols[1] != "docs" {
		t.Errorf("expected [method docs], got %v", cols)
	}
	help := flattenRows(rows)
	for _, want := range []string{"find", "aggregate", "distinct", "/method/db.collection.find/"} {
		if !strings.Contains(help, want) {
			t.Errorf("missing %q in collection help", want)
		}
	}
}

func TestMethodHelp(t *testing.T) {
	cols, rows := MethodHelp("find")
	if cols[0] != "chainer" {
		t.Errorf("expected [chainer] column, got %v", cols)
	}
	help := flattenRows(rows)
	for _, want := range []string{"find() chainers:", "sort", "limit", "skip"} {
		if !strings.Contains(help, want) {
			t.Errorf("missing %q in method help", want)
		}
	}
}

func TestMethodHelp_Unknown(t *testing.T) {
	_, rows := MethodHelp("unknown")
	if !strings.Contains(flattenRows(rows), "Unknown method") {
		t.Error("expected 'Unknown method' for unknown method")
	}
}

func TestPrintAllHelp(t *testing.T) {
	logHelp := func(title string, cols []string, rows [][]string) {
		t.Logf("\n=== %s ===", title)

		widths := make([]int, len(cols))
		for i, c := range cols {
			widths[i] = len(c)
		}
		for _, row := range rows {
			for i, cell := range row {
				if i < len(widths) && len(cell) > widths[i] {
					widths[i] = len(cell)
				}
			}
		}

		header := make([]string, len(cols))
		sep := make([]string, len(cols))
		for i, c := range cols {
			header[i] = fmt.Sprintf(" %-*s ", widths[i], c)
			sep[i] = strings.Repeat("-", widths[i]+2)
		}
		t.Log(strings.Join(header, "|"))
		t.Log(strings.Join(sep, "+"))

		for _, row := range rows {
			cells := make([]string, len(cols))
			for i := range cols {
				v := ""
				if i < len(row) {
					v = row[i]
				}
				cells[i] = fmt.Sprintf(" %-*s ", widths[i], v)
			}
			t.Log(strings.Join(cells, "|"))
		}
	}
	c, r := GlobalHelp()
	logHelp("help()", c, r)
	c, r = WireCommandHelp()
	logHelp("helpCommands()", c, r)
	c, r = DatabaseHelp()
	logHelp("db.help()", c, r)
	c, r = CollectionHelp()
	logHelp("db.coll.help()", c, r)
	c, r = MethodHelp("find")
	logHelp("db.coll.find().help()", c, r)
	c, r = MethodHelp("aggregate")
	logHelp("db.coll.aggregate().help()", c, r)
}
