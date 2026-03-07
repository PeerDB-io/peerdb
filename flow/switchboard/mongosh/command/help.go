package command

import (
	"sort"
)

const docsBaseURL = "https://www.mongodb.com/docs/manual/reference"

// GlobalHelp returns help for the help command — input format and allowed wire commands with doc links.
func GlobalHelp() ([]string, [][]string) {
	rows := [][]string{
		{"Input Format:", ""},
		{`  Extended JSON wire commands`, `e.g. {"find": "coll", "filter": {}}`},
		{"", ""},
		{"Allowed Wire Commands:", ""},
	}

	sections := make([]string, 0, len(AllowedCommands))
	for section := range AllowedCommands {
		sections = append(sections, section)
	}
	sort.Strings(sections)

	for i, section := range sections {
		if i > 0 {
			rows = append(rows, []string{"", ""})
		}
		rows = append(rows, []string{"  " + section + ":", ""})
		for _, cmd := range AllowedCommands[section] {
			rows = append(rows, []string{"    " + cmd.Name, docsBaseURL + "/command/" + cmd.Name + "/"})
		}
	}
	return []string{"command", "docs"}, rows
}
