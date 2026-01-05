package command

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// ParseArgsAccordingTo parses raw argument strings according to the expected kinds.
// It handles optional arguments and type conversion.
func ParseArgsAccordingTo(kinds []ArgKind, rawArgs []string) ([]any, error) {
	result := make([]any, 0, len(kinds))
	argIdx := 0
	kindIdx := 0

	for kindIdx < len(kinds) {
		kind := kinds[kindIdx]

		// Check if this argument is optional
		isOptional := false
		actualKind := kind
		if kind == ArgOptional {
			isOptional = true
			kindIdx++
			if kindIdx < len(kinds) {
				actualKind = kinds[kindIdx]
			} else {
				// ArgOptional at the end without actual type
				break
			}
		}

		// If we're out of arguments
		if argIdx >= len(rawArgs) {
			if isOptional {
				result = append(result, nil)
				kindIdx++
				continue
			}
			// Non-optional argument missing
			return nil, fmt.Errorf("expected at least %d arguments, got %d", countRequiredArgs(kinds), len(rawArgs))
		}

		// Parse the argument according to its kind
		parsed, err := parseArgument(rawArgs[argIdx], actualKind)
		if err != nil {
			return nil, fmt.Errorf("argument %d: %w", argIdx+1, err)
		}

		result = append(result, parsed)
		argIdx++
		kindIdx++
	}

	// Check if there are extra arguments (only if no optionals)
	if argIdx < len(rawArgs) && !hasOptionalArgs(kinds) {
		return nil, fmt.Errorf("too many arguments: expected %d, got %d", countNonOptionalArgs(kinds), len(rawArgs))
	}

	return result, nil
}

// parseArgument parses a single argument according to its expected kind.
func parseArgument(raw string, kind ArgKind) (any, error) {
	raw = strings.TrimSpace(raw)

	switch kind {
	case ArgJSON:
		// Arguments from the AST parser are already in Extended JSON v2 format
		// No need to rewrite literals as they've been handled by the parser

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
func countRequiredArgs(kinds []ArgKind) int {
	count := 0
	for i := 0; i < len(kinds); i++ {
		if kinds[i] != ArgOptional {
			count++
		} else if i+1 < len(kinds) {
			// Skip the next kind since it's marked as optional
			i++
		}
	}
	return count
}

// hasOptionalArgs checks if the kinds array contains any optional arguments.
func hasOptionalArgs(kinds []ArgKind) bool {
	for _, k := range kinds {
		if k == ArgOptional {
			return true
		}
	}
	return false
}

// countNonOptionalArgs counts the total number of arguments excluding ArgOptional markers.
func countNonOptionalArgs(kinds []ArgKind) int {
	count := 0
	for _, k := range kinds {
		if k != ArgOptional {
			count++
		}
	}
	return count
}
