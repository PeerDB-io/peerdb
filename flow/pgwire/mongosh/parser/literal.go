package parser

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/dop251/goja/ast"
	"github.com/dop251/goja/token"
)

// keyValue represents a key-value pair
type keyValue struct {
	Key   string
	Value interface{}
}

// orderedMap is a slice of key-value pairs that preserves insertion order
type orderedMap []keyValue

// MarshalJSON implements json.Marshaler for orderedMap
func (om orderedMap) MarshalJSON() ([]byte, error) {
	if len(om) == 0 {
		return []byte("{}"), nil
	}

	var parts []string
	for _, kv := range om {
		// Marshal key
		keyJSON, err := json.Marshal(kv.Key)
		if err != nil {
			return nil, err
		}

		// Marshal value (this will recursively handle nested orderedMaps)
		valJSON, err := json.Marshal(kv.Value)
		if err != nil {
			return nil, err
		}

		parts = append(parts, fmt.Sprintf("%s:%s", keyJSON, valJSON))
	}

	return []byte("{" + strings.Join(parts, ",") + "}"), nil
}

// evalLiteral evaluates an AST expression to a Go value
func evalLiteral(expr ast.Expression) (any, error) {
	switch e := expr.(type) {
	case *ast.NullLiteral:
		return nil, nil

	case *ast.BooleanLiteral:
		return e.Value, nil

	case *ast.NumberLiteral:
		// Try to preserve integers when possible
		switch v := e.Value.(type) {
		case float64:
			if v == float64(int64(v)) {
				return int64(v), nil
			}
			return v, nil
		case int64:
			return v, nil
		case int:
			return int64(v), nil
		default:
			// Try to convert to float64
			return e.Value, nil
		}

	case *ast.StringLiteral:
		return string(e.Value), nil

	case *ast.RegExpLiteral:
		// Convert to MongoDB Extended JSON format
		return map[string]any{
			"$regularExpression": map[string]any{
				"pattern": e.Pattern,
				"options": e.Flags,
			},
		}, nil

	case *ast.ArrayLiteral:
		arr := make([]any, 0, len(e.Value))
		for _, elem := range e.Value {
			val, err := evalLiteral(elem)
			if err != nil {
				return nil, err
			}
			arr = append(arr, val)
		}
		return arr, nil

	case *ast.ObjectLiteral:
		// For objects, we need to preserve order for MongoDB commands
		// Build an ordered list of key-value pairs
		pairs := make([]keyValue, 0, len(e.Value))
		for _, prop := range e.Value {
			if prop, ok := prop.(*ast.PropertyKeyed); ok {
				// Get the key
				var key string
				switch k := prop.Key.(type) {
				case *ast.StringLiteral:
					key = string(k.Value)
				case *ast.Identifier:
					key = string(k.Name)
				default:
					// Try to evaluate the key
					val, err := evalLiteral(k)
					if err != nil {
						return nil, err
					}
					key = fmt.Sprint(val)
				}

				// Get the value
				val, err := evalLiteral(prop.Value)
				if err != nil {
					return nil, err
				}

				// Add key-value pair
				pairs = append(pairs, keyValue{key, val})
			}
		}
		// Return as an orderedMap that preserves field order
		return orderedMap(pairs), nil

	case *ast.UnaryExpression:
		// Handle negative numbers
		if e.Operator == token.MINUS {
			val, err := evalLiteral(e.Operand)
			if err != nil {
				return nil, err
			}
			switch v := val.(type) {
			case float64:
				return -v, nil
			case int64:
				return -v, nil
			}
		}
		return nil, fmt.Errorf("unsupported unary operator: %s", e.Operator)

	case *ast.CallExpression:
		// Handle BSON/EJSON constructors
		return evalBSONConstructor(e)

	case *ast.NewExpression:
		// Handle new Date(), new RegExp(), etc.
		return evalNewExpression(e)

	case *ast.Identifier:
		// Handle special identifiers
		if e.Name == "undefined" {
			return map[string]any{"$undefined": true}, nil
		}
		return nil, fmt.Errorf("unsupported identifier: %s", e.Name)

	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// evalBSONConstructor evaluates BSON constructor calls
func evalBSONConstructor(call *ast.CallExpression) (any, error) {
	// Get the function name
	var fnName string
	switch callee := call.Callee.(type) {
	case *ast.Identifier:
		fnName = string(callee.Name)
	default:
		return nil, fmt.Errorf("unsupported call expression: %T", call.Callee)
	}

	// Handle each constructor type
	switch fnName {
	case "ObjectId":
		if len(call.ArgumentList) != 1 {
			return nil, fmt.Errorf("ObjectId() requires exactly 1 argument, got %d", len(call.ArgumentList))
		}
		arg, err := evalLiteral(call.ArgumentList[0])
		if err != nil {
			return nil, err
		}
		if str, ok := arg.(string); ok {
			return map[string]any{"$oid": str}, nil
		}
		return nil, fmt.Errorf("ObjectId() argument must be a string, got %T", arg)

	case "ISODate", "Date":
		if len(call.ArgumentList) != 1 {
			return nil, fmt.Errorf("%s() requires exactly 1 argument, got %d", fnName, len(call.ArgumentList))
		}
		arg, err := evalLiteral(call.ArgumentList[0])
		if err != nil {
			return nil, err
		}
		switch v := arg.(type) {
		case string:
			// Validate: must be full ISO 8601 format with time component
			if !strings.Contains(v, "T") {
				return nil, fmt.Errorf("%s() requires full ISO 8601 format (e.g., \"2021-01-01T00:00:00Z\"), got %q", fnName, v)
			}
			return map[string]any{"$date": v}, nil
		case float64:
			// Epoch milliseconds
			return map[string]any{
				"$date": map[string]any{
					"$numberLong": strconv.FormatInt(int64(v), 10),
				},
			}, nil
		case int64:
			// Epoch milliseconds
			return map[string]any{
				"$date": map[string]any{
					"$numberLong": strconv.FormatInt(v, 10),
				},
			}, nil
		case nil:
			// null is treated as epoch 0
			return map[string]any{
				"$date": map[string]any{
					"$numberLong": "0",
				},
			}, nil
		}
		return nil, fmt.Errorf("%s() argument must be a string or number, got %T", fnName, arg)

	case "UUID":
		if len(call.ArgumentList) != 1 {
			return nil, fmt.Errorf("UUID() requires exactly 1 argument, got %d", len(call.ArgumentList))
		}
		arg, err := evalLiteral(call.ArgumentList[0])
		if err != nil {
			return nil, err
		}
		if str, ok := arg.(string); ok {
			return map[string]any{"$uuid": str}, nil
		}
		return nil, fmt.Errorf("UUID() argument must be a string, got %T", arg)

	case "BinData":
		if len(call.ArgumentList) != 2 {
			return nil, fmt.Errorf("BinData() requires exactly 2 arguments, got %d", len(call.ArgumentList))
		}
		subType, err := evalLiteral(call.ArgumentList[0])
		if err != nil {
			return nil, err
		}
		base64, err := evalLiteral(call.ArgumentList[1])
		if err != nil {
			return nil, err
		}
		// First arg must be numeric
		if !isNumeric(subType) {
			return nil, fmt.Errorf("BinData() first argument must be a number, got %T", subType)
		}
		// Second arg must be string
		base64Str, ok := base64.(string)
		if !ok {
			return nil, fmt.Errorf("BinData() second argument must be a string, got %T", base64)
		}
		subTypeStr := fmt.Sprintf("%02x", subType)
		if st, ok := subType.(int64); ok {
			subTypeStr = fmt.Sprintf("%02x", st)
		}
		return map[string]any{
			"$binary": map[string]any{
				"base64":  base64Str,
				"subType": subTypeStr,
			},
		}, nil

	case "Timestamp":
		if len(call.ArgumentList) != 2 {
			return nil, fmt.Errorf("Timestamp() requires exactly 2 arguments, got %d", len(call.ArgumentList))
		}
		t, err := evalLiteral(call.ArgumentList[0])
		if err != nil {
			return nil, err
		}
		i, err := evalLiteral(call.ArgumentList[1])
		if err != nil {
			return nil, err
		}
		// Validate both are numeric
		if !isNumeric(t) {
			return nil, fmt.Errorf("Timestamp() first argument must be a number, got %T", t)
		}
		if !isNumeric(i) {
			return nil, fmt.Errorf("Timestamp() second argument must be a number, got %T", i)
		}
		return map[string]any{
			"$timestamp": map[string]any{
				"t": t,
				"i": i,
			},
		}, nil

	case "NumberInt", "Int32":
		if len(call.ArgumentList) != 1 {
			return nil, fmt.Errorf("%s() requires exactly 1 argument, got %d", fnName, len(call.ArgumentList))
		}
		arg, err := evalLiteral(call.ArgumentList[0])
		if err != nil {
			return nil, err
		}
		switch v := arg.(type) {
		case float64:
			return map[string]any{"$numberInt": strconv.FormatInt(int64(v), 10)}, nil
		case int64:
			return map[string]any{"$numberInt": strconv.FormatInt(v, 10)}, nil
		case string:
			return map[string]any{"$numberInt": v}, nil
		}
		return nil, fmt.Errorf("%s() argument must be a number or string, got %T", fnName, arg)

	case "NumberLong", "Long":
		if len(call.ArgumentList) != 1 {
			return nil, fmt.Errorf("%s() requires exactly 1 argument, got %d", fnName, len(call.ArgumentList))
		}
		arg, err := evalLiteral(call.ArgumentList[0])
		if err != nil {
			return nil, err
		}
		switch v := arg.(type) {
		case float64:
			return map[string]any{"$numberLong": strconv.FormatInt(int64(v), 10)}, nil
		case int64:
			return map[string]any{"$numberLong": strconv.FormatInt(v, 10)}, nil
		case string:
			return map[string]any{"$numberLong": v}, nil
		}
		return nil, fmt.Errorf("%s() argument must be a number or string, got %T", fnName, arg)

	case "Double", "NumberDouble":
		if len(call.ArgumentList) != 1 {
			return nil, fmt.Errorf("%s() requires exactly 1 argument, got %d", fnName, len(call.ArgumentList))
		}
		arg, err := evalLiteral(call.ArgumentList[0])
		if err != nil {
			return nil, err
		}
		switch v := arg.(type) {
		case float64:
			return map[string]any{"$numberDouble": fmt.Sprint(v)}, nil
		case int64:
			return map[string]any{"$numberDouble": fmt.Sprint(v)}, nil
		case string:
			return map[string]any{"$numberDouble": v}, nil
		}
		return nil, fmt.Errorf("%s() argument must be a number or string, got %T", fnName, arg)

	case "NumberDecimal", "Decimal128":
		if len(call.ArgumentList) != 1 {
			return nil, fmt.Errorf("%s() requires exactly 1 argument, got %d", fnName, len(call.ArgumentList))
		}
		arg, err := evalLiteral(call.ArgumentList[0])
		if err != nil {
			return nil, err
		}
		switch v := arg.(type) {
		case string:
			return map[string]any{"$numberDecimal": v}, nil
		case float64:
			return map[string]any{"$numberDecimal": fmt.Sprint(v)}, nil
		case int64:
			return map[string]any{"$numberDecimal": fmt.Sprint(v)}, nil
		}
		return nil, fmt.Errorf("%s() argument must be a number or string, got %T", fnName, arg)

	case "MinKey":
		if len(call.ArgumentList) != 0 {
			return nil, fmt.Errorf("MinKey() takes no arguments, got %d", len(call.ArgumentList))
		}
		return map[string]any{"$minKey": 1}, nil

	case "MaxKey":
		if len(call.ArgumentList) != 0 {
			return nil, fmt.Errorf("MaxKey() takes no arguments, got %d", len(call.ArgumentList))
		}
		return map[string]any{"$maxKey": 1}, nil
	}

	return nil, fmt.Errorf("unsupported constructor: %s", fnName)
}

// evalNewExpression evaluates new expressions
func evalNewExpression(expr *ast.NewExpression) (any, error) {
	// Get the constructor name
	var ctorName string
	switch callee := expr.Callee.(type) {
	case *ast.Identifier:
		ctorName = string(callee.Name)
	default:
		return nil, fmt.Errorf("unsupported new expression: %T", expr.Callee)
	}

	// Handle each constructor type
	switch ctorName {
	case "Date", "ISODate":
		if len(expr.ArgumentList) == 0 {
			// new Date() with no args means current date
			return map[string]any{"$date": "now"}, nil
		}
		if len(expr.ArgumentList) == 1 {
			arg, err := evalLiteral(expr.ArgumentList[0])
			if err != nil {
				return nil, err
			}
			switch v := arg.(type) {
			case string:
				if !strings.Contains(v, "T") {
					return nil, fmt.Errorf("new %s() requires full ISO 8601 format (e.g., \"2021-01-01T00:00:00Z\"), got %q", ctorName, v)
				}
				return map[string]any{"$date": v}, nil
			case float64:
				return map[string]any{
					"$date": map[string]any{
						"$numberLong": strconv.FormatInt(int64(v), 10),
					},
				}, nil
			case int64:
				return map[string]any{
					"$date": map[string]any{
						"$numberLong": strconv.FormatInt(v, 10),
					},
				}, nil
			}
			return nil, fmt.Errorf("new %s() argument must be a string or number, got %T", ctorName, arg)
		}
		return nil, fmt.Errorf("new %s() requires 0 or 1 argument, got %d", ctorName, len(expr.ArgumentList))

	case "RegExp":
		if len(expr.ArgumentList) < 1 {
			return nil, fmt.Errorf("new RegExp() requires at least 1 argument")
		}
		pattern, err := evalLiteral(expr.ArgumentList[0])
		if err != nil {
			return nil, err
		}
		patternStr, ok := pattern.(string)
		if !ok {
			return nil, fmt.Errorf("new RegExp() pattern must be a string, got %T", pattern)
		}
		flags := ""
		if len(expr.ArgumentList) >= 2 {
			f, err := evalLiteral(expr.ArgumentList[1])
			if err != nil {
				return nil, err
			}
			if flagStr, ok := f.(string); ok {
				flags = flagStr
			}
		}
		return map[string]any{
			"$regularExpression": map[string]any{
				"pattern": patternStr,
				"options": flags,
			},
		}, nil
	}

	return nil, fmt.Errorf("unsupported constructor: new %s()", ctorName)
}

// exprToJSON converts an AST expression to a JSON string
func exprToJSON(expr ast.Expression) (string, error) {
	val, err := evalLiteral(expr)
	if err != nil {
		return "", err
	}

	return valueToJSON(val), nil
}

// valueToJSON converts a value to JSON string
func valueToJSON(val interface{}) string {
	// Convert to JSON (orderedMap has MarshalJSON so it will be handled correctly)
	jsonBytes, err := json.Marshal(val)
	if err != nil {
		return ""
	}

	return string(jsonBytes)
}

// isNumeric returns true if the value is a numeric type
func isNumeric(v any) bool {
	switch v.(type) {
	case int, int32, int64, float32, float64:
		return true
	default:
		return false
	}
}
