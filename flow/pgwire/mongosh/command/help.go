package command

import (
	"fmt"
	"sort"
	"strings"
)

// GlobalHelp returns help for help() - overview of shell methods and wire commands.
func GlobalHelp() []string {
	lines := []string{
		"Shell Commands:",
		"  use <database>      - switch to a different database",
		"  show collections    - list collections in current database",
		"  show databases      - list all databases",
		"  show dbs            - alias for show databases",
		"",
		"Shell Methods:",
	}

	// Group by scope
	var dbMethods, collMethods []string
	for _, spec := range Registry {
		if spec.Scope == "database" {
			dbMethods = append(dbMethods, spec.Name)
		} else {
			collMethods = append(collMethods, spec.Name)
		}
	}
	sort.Strings(dbMethods)
	sort.Strings(collMethods)

	lines = append(lines,
		"  db.<method>(): "+strings.Join(dbMethods, ", "),
		"  db.<coll>.<method>(): "+strings.Join(collMethods, ", "),
		"",
		"Wire Commands:",
	)

	// Sort sections
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
		lines = append(lines, "  "+section+": "+strings.Join(names, ", "))
	}

	lines = append(lines,
		"",
		"Use db.help(), db.<coll>.help(), db.<coll>.find().help() for details.",
	)
	return lines
}

// DatabaseHelp returns help for db.help() - database-level methods.
func DatabaseHelp() []string {
	lines := []string{"Database Methods:"}
	for _, spec := range Registry {
		if spec.Scope == "database" {
			lines = append(lines, fmt.Sprintf("  db.%s(%s)", spec.Name, formatArgs(spec.Args)))
		}
	}
	return lines
}

// CollectionHelp returns help for db.coll.help() - collection-level methods.
func CollectionHelp() []string {
	lines := []string{"Collection Methods:"}

	// Collect and sort by display name
	var specs []MethodSpec
	for _, spec := range Registry {
		if spec.Scope == "collection" {
			specs = append(specs, spec)
		}
	}
	sort.Slice(specs, func(i, j int) bool {
		return specs[i].Name < specs[j].Name
	})

	for _, spec := range specs {
		lines = append(lines, fmt.Sprintf("  .%s(%s)", spec.Name, formatArgs(spec.Args)))
	}
	lines = append(lines,
		"",
		"Use db.<coll>.<method>().help() for available chainers.",
	)
	return lines
}

// MethodHelp returns help for db.coll.find().help() - chainers for a specific method.
func MethodHelp(method string) []string {
	spec, ok := Registry[strings.ToLower(method)]
	if !ok {
		return []string{"Unknown method: " + method}
	}

	lines := []string{spec.Name + "() chainers:"}

	if len(spec.Chainers) == 0 {
		lines = append(lines, "  (none)")
		return lines
	}

	// Collect and sort chainers by name
	chainers := make([]ChainerSpec, 0, len(spec.Chainers))
	for _, chainer := range spec.Chainers {
		chainers = append(chainers, chainer)
	}
	sort.Slice(chainers, func(i, j int) bool {
		return chainers[i].Name < chainers[j].Name
	})

	for _, chainer := range chainers {
		lines = append(lines, fmt.Sprintf("  .%s(%s)", chainer.Name, formatArgs(chainer.Args)))
	}
	return lines
}

// formatArgs formats argument kinds for display.
func formatArgs(args []ArgKind) string {
	if len(args) == 0 {
		return ""
	}

	parts := make([]string, 0, len(args))
	optional := false
	for _, arg := range args {
		if arg == ArgOptional {
			optional = true
			continue
		}
		name := argKindName(arg)
		if optional {
			name = "[" + name + "]"
			optional = false
		}
		parts = append(parts, name)
	}
	return strings.Join(parts, ", ")
}

func argKindName(kind ArgKind) string {
	switch kind {
	case ArgJSON:
		return "doc"
	case ArgString:
		return "str"
	case ArgInt:
		return "n"
	case ArgBool:
		return "bool"
	default:
		return "arg"
	}
}
