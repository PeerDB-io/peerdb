package command

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// ParseArgsAccordingTo parses raw argument strings according to the expected specs.
// It handles optional arguments and type conversion.
func ParseArgsAccordingTo(specs []ArgSpec, rawArgs []string) ([]any, error) {
	result := make([]any, 0, len(specs))
	argIdx := 0

	for _, spec := range specs {
		// If we're out of arguments
		if argIdx >= len(rawArgs) {
			if spec.Optional {
				result = append(result, nil)
				continue
			}
			// Non-optional argument missing
			return nil, fmt.Errorf("expected at least %d arguments, got %d", countRequiredArgs(specs), len(rawArgs))
		}

		// Parse the argument according to its kind
		parsed, err := parseArgument(rawArgs[argIdx], spec.Kind)
		if err != nil {
			return nil, fmt.Errorf("argument %d (%s): %w", argIdx+1, spec.Name, err)
		}

		result = append(result, parsed)
		argIdx++
	}

	// Check if there are extra arguments (only if no optionals)
	if argIdx < len(rawArgs) && !hasOptionalArgs(specs) {
		return nil, fmt.Errorf("too many arguments: expected %d, got %d", len(specs), len(rawArgs))
	}

	return result, nil
}

// parseArgument parses a single argument according to its expected kind.
func parseArgument(raw string, kind ArgKind) (any, error) {
	raw = strings.TrimSpace(raw)

	switch kind {
	case ArgJSON:
		// Arguments from the AST parser are already in Extended JSON v2 format

		// Parse as Extended JSON
		var doc interface{}
		if err := bson.UnmarshalExtJSON([]byte(raw), false, &doc); err != nil {
			// Try as regular JSON
			if err := json.Unmarshal([]byte(raw), &doc); err != nil {
				return nil, fmt.Errorf("invalid JSON: %w", err)
			}
		}
		return doc, nil

	case ArgString:
		// Remove surrounding quotes if present
		if len(raw) >= 2 && ((raw[0] == '"' && raw[len(raw)-1] == '"') ||
			(raw[0] == '\'' && raw[len(raw)-1] == '\'')) {
			return raw[1 : len(raw)-1], nil
		}
		return raw, nil

	case ArgInt:
		val, err := strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("expected integer, got %q", raw)
		}
		return val, nil

	case ArgBool:
		switch strings.ToLower(raw) {
		case "true":
			return true, nil
		case "false":
			return false, nil
		default:
			return nil, fmt.Errorf("expected boolean, got %q", raw)
		}

	default:
		return nil, fmt.Errorf("unknown argument kind: %v", kind)
	}
}

// countRequiredArgs counts the number of required (non-optional) arguments.
func countRequiredArgs(specs []ArgSpec) int {
	count := 0
	for _, s := range specs {
		if !s.Optional {
			count++
		}
	}
	return count
}

// hasOptionalArgs checks if the specs contain any optional arguments.
func hasOptionalArgs(specs []ArgSpec) bool {
	for _, s := range specs {
		if s.Optional {
			return true
		}
	}
	return false
}
