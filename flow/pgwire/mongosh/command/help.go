package command

import (
	"fmt"
	"sort"
	"strings"
)

// GlobalHelp returns help for help() - overview of shell methods and wire commands.
func GlobalHelp() string {
	var sb strings.Builder
	sb.WriteString("Shell Methods:\n")

	// Group by scope
	var dbMethods, collMethods []string
	for name, spec := range Registry {
		if spec.Scope == "database" {
			dbMethods = append(dbMethods, name)
		} else {
			collMethods = append(collMethods, name)
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

	for name, spec := range Registry {
		if spec.Scope == "database" {
			sb.WriteString(fmt.Sprintf("  db.%s(%s)\n", name, formatArgs(spec.Args)))
		}
	}
	return sb.String()
}

// CollectionHelp returns help for db.coll.help() - collection-level methods.
func CollectionHelp() string {
	var sb strings.Builder
	sb.WriteString("Collection Methods:\n")

	var methods []string
	for name, spec := range Registry {
		if spec.Scope == "collection" {
			methods = append(methods, name)
		}
	}
	sort.Strings(methods)

	for _, name := range methods {
		spec := Registry[name]
		sb.WriteString(fmt.Sprintf("  .%s(%s)", name, formatArgs(spec.Args)))
		if len(spec.Chainers) > 0 {
			var chainers []string
			for c := range spec.Chainers {
				chainers = append(chainers, c)
			}
			sort.Strings(chainers)
			sb.WriteString(fmt.Sprintf(" [%s]", strings.Join(chainers, ", ")))
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

// MethodHelp returns help for db.coll.find().help() - chainers for a specific method.
func MethodHelp(method string) string {
	spec, ok := Registry[strings.ToLower(method)]
	if !ok {
		return fmt.Sprintf("Unknown method: %s\n", method)
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%s() chainers:\n", method))

	if len(spec.Chainers) == 0 {
		sb.WriteString("  (none)\n")
		return sb.String()
	}

	var chainers []string
	for name := range spec.Chainers {
		chainers = append(chainers, name)
	}
	sort.Strings(chainers)

	for _, name := range chainers {
		chainer := spec.Chainers[name]
		sb.WriteString(fmt.Sprintf("  .%s(%s)\n", name, formatArgs(chainer.Args)))
	}
	return sb.String()
}

// formatArgs formats argument kinds for display.
func formatArgs(args []ArgKind) string {
	if len(args) == 0 {
		return ""
	}

	var parts []string
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
