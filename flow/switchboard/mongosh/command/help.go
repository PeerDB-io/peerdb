package command

import (
	"fmt"
	"sort"
	"strings"
)

const docsBaseURL = "https://www.mongodb.com/docs/manual/reference"

// GlobalHelp returns help for help() - overview of shell methods and wire commands.
func GlobalHelp() ([]string, [][]string) {
	rows := [][]string{
		{"Shell Commands:"},
		{"  show collections    - list collections in current database"},
		{"  show databases      - list all databases"},
		{"  show dbs            - alias for show databases"},
		{""},
		{"Shell Methods:"},
	}

	var dbMethods, collMethods []string
	for _, spec := range Registry {
		if spec.Scope == ScopeDatabase {
			dbMethods = append(dbMethods, spec.Name)
		} else {
			collMethods = append(collMethods, spec.Name)
		}
	}
	sort.Strings(dbMethods)
	sort.Strings(collMethods)

	rows = append(rows,
		[]string{"  db.<method>(): " + strings.Join(dbMethods, ", ")},
		[]string{"  db.<coll>.<method>(): " + strings.Join(collMethods, ", ")},
		[]string{""},
		[]string{"Wire Commands:"},
	)

	sections := make([]string, 0, len(AllowedCommands))
	for section := range AllowedCommands {
		sections = append(sections, section)
	}
	sort.Strings(sections)

	for _, section := range sections {
		cmds := AllowedCommands[section]
		names := make([]string, len(cmds))
		for i, cmd := range cmds {
			names[i] = cmd.Name
		}
		rows = append(rows, []string{
			"  " + section + ": " + strings.Join(names, ", "),
		})
	}

	rows = append(rows,
		[]string{""},
		[]string{"Use helpCommands() for wire command docs."},
		[]string{"Use db.help(), db.<coll>.help(), db.<coll>.find().help() for method details."},
	)
	return []string{"help"}, rows
}

// WireCommandHelp returns help for helpCommands() - categorized wire commands with doc links.
func WireCommandHelp() ([]string, [][]string) {
	var rows [][]string

	sections := make([]string, 0, len(AllowedCommands))
	for section := range AllowedCommands {
		sections = append(sections, section)
	}
	sort.Strings(sections)

	for i, section := range sections {
		if i > 0 {
			rows = append(rows, []string{"", ""})
		}
		rows = append(rows, []string{section + ":", ""})
		for _, cmd := range AllowedCommands[section] {
			rows = append(rows, []string{"  " + cmd.Name, docsBaseURL + "/command/" + cmd.Name + "/"})
		}
	}
	return []string{"command", "docs"}, rows
}

// DatabaseHelp returns help for db.help() - database-level methods.
func DatabaseHelp() ([]string, [][]string) {
	var rows [][]string
	for _, spec := range Registry {
		if spec.Scope == ScopeDatabase {
			sig := fmt.Sprintf("db.%s(%s)", spec.Name, formatArgs(spec.Args))
			rows = append(rows, []string{sig, docsBaseURL + "/method/db." + spec.Name + "/"})
		}
	}
	return []string{"method", "docs"}, rows
}

// CollectionHelp returns help for db.coll.help() - collection-level methods.
func CollectionHelp() ([]string, [][]string) {
	var specs []MethodSpec
	for _, spec := range Registry {
		if spec.Scope == ScopeCollection {
			specs = append(specs, spec)
		}
	}
	sort.Slice(specs, func(i, j int) bool {
		return specs[i].Name < specs[j].Name
	})

	rows := make([][]string, len(specs))
	for i, spec := range specs {
		sig := fmt.Sprintf(".%s(%s)", spec.Name, formatArgs(spec.Args))
		rows[i] = []string{sig, docsBaseURL + "/method/db.collection." + spec.Name + "/"}
	}
	return []string{"method", "docs"}, rows
}

// MethodHelp returns help for db.coll.find().help() - chainers for a specific method.
func MethodHelp(method string) ([]string, [][]string) {
	spec, ok := Registry[strings.ToLower(method)]
	if !ok {
		return []string{"help"}, [][]string{{"Unknown method: " + method}}
	}

	rows := [][]string{{spec.Name + "() chainers:"}}

	if len(spec.Chainers) == 0 {
		rows = append(rows, []string{"  (none)"})
		return []string{"chainer"}, rows
	}

	chainers := make([]ChainerSpec, 0, len(spec.Chainers))
	for _, chainer := range spec.Chainers {
		chainers = append(chainers, chainer)
	}
	sort.Slice(chainers, func(i, j int) bool {
		return chainers[i].Name < chainers[j].Name
	})

	for _, chainer := range chainers {
		rows = append(rows, []string{fmt.Sprintf("  .%s(%s)", chainer.Name, formatArgs(chainer.Args))})
	}
	return []string{"chainer"}, rows
}

func formatArgs(args []ArgSpec) string {
	if len(args) == 0 {
		return ""
	}

	parts := make([]string, 0, len(args))
	for _, arg := range args {
		name := arg.Name
		if arg.Optional {
			name = "[" + name + "]"
		}
		parts = append(parts, name)
	}
	return strings.Join(parts, ", ")
}
