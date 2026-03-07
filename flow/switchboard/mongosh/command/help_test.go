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
	if cols[0] != "command" || cols[1] != "docs" {
		t.Errorf("expected [command docs], got %v", cols)
	}
	help := flattenRows(rows)
	wants := []string{
		"Allowed Wire Commands:", "Input Format:",
		"ping", "find", "/reference/command/", "listCollections", "listDatabases",
	}
	for _, want := range wants {
		if !strings.Contains(help, want) {
			t.Errorf("missing %q in global help", want)
		}
	}
}

func TestPrintAllHelp(t *testing.T) {
	cols, rows := GlobalHelp()

	t.Logf("\n=== help() ===")
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
