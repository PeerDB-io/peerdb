package command

import (
	"fmt"
	"sort"
	"strings"
)

// GlobalHelp returns help for help() - overview of shell methods and wire commands.
func GlobalHelp() string {
	var sb strings.Builder
	sb.WriteString("Shell Commands:\n")
	sb.WriteString("  show collections    - list collections in current database\n")
	sb.WriteString("  show databases      - list all databases\n")
	sb.WriteString("  show dbs            - alias for show databases\n")

	sb.WriteString("\nShell Methods:\n")

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

	sb.WriteString("  db.<method>(): ")
	sb.WriteString(strings.Join(dbMethods, ", "))
	sb.WriteString("\n  db.<coll>.<method>(): ")
	sb.WriteString(strings.Join(collMethods, ", "))
	sb.WriteString("\n\nWire Commands:\n")

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
		sb.WriteString(fmt.Sprintf("  %s: %s\n", section, strings.Join(names, ", ")))
	}

	sb.WriteString("\nUse db.help(), db.<coll>.help(), db.<coll>.find().help() for details.\n")
	return sb.String()
}

// DatabaseHelp returns help for db.help() - database-level methods.
func DatabaseHelp() string {
	var sb strings.Builder
	sb.WriteString("Database Methods:\n")

	for _, spec := range Registry {
		if spec.Scope == "database" {
			fmt.Fprintf(&sb, "  db.%s(%s)\n", spec.Name, formatArgs(spec.Args))
		}
	}
	return sb.String()
}

// CollectionHelp returns help for db.coll.help() - collection-level methods.
func CollectionHelp() string {
	var sb strings.Builder
	sb.WriteString("Collection Methods:\n")

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
		fmt.Fprintf(&sb, "  .%s(%s)\n", spec.Name, formatArgs(spec.Args))
	}
	sb.WriteString("\nUse db.<coll>.<method>().help() for available chainers.\n")
	return sb.String()
}

// MethodHelp returns help for db.coll.find().help() - chainers for a specific method.
func MethodHelp(method string) string {
	spec, ok := Registry[strings.ToLower(method)]
	if !ok {
		return fmt.Sprintf("Unknown method: %s\n", method)
	}

	var sb strings.Builder
	sb.WriteString(spec.Name + "() chainers:\n")

	if len(spec.Chainers) == 0 {
		sb.WriteString("  (none)\n")
		return sb.String()
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
		fmt.Fprintf(&sb, "  .%s(%s)\n", chainer.Name, formatArgs(chainer.Args))
	}
	return sb.String()
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
