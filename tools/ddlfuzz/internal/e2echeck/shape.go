package e2echeck

import "strings"

func ShapeFor(class string, meta map[string]any) string {
	switch class {
	case ClassStatusVarWalk:
		return "status-vars"
	case ClassSQLModeMismatch:
		return "sql-mode"
	case ClassQueryRewrite:
		return "query-rewrite"
	case ClassPlumbingSig:
		return "plumbing-sig"
	case ClassPanic:
		return "parser-panic"
	case ClassParseErrorLiveAccept:
		return "parse-error-live-accept"
	case ClassPositionMissed:
		return "position-missed"
	case ClassColumnAttr:
		attr, _ := meta["attribute"].(string)
		switch attr {
		case "rename_column_type", "qkind", "nullability", "numeric_precision", "numeric_scale":
			return "col-attr(" + attr + ")"
		default:
			return "col-attr(?)"
		}
	case ClassMissedColumnEffect:
		for k := range meta {
			switch {
			case strings.HasPrefix(k, "added_"):
				return "missed-effect(added)"
			case strings.HasPrefix(k, "dropped_"):
				return "missed-effect(dropped)"
			}
		}
		if _, ok := meta["changed_unexpected"]; ok {
			return "missed-effect(changed)"
		}
		if _, ok := meta["benign_classification"]; ok {
			return "missed-effect(benign)"
		}
		return "missed-effect(?)"
	default:
		return ""
	}
}
