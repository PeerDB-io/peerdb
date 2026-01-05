package command

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// FindBase creates a find command builder.
func FindBase(onlyOne bool) BuildFunc {
	return func(collection string, args []any) (bson.D, ExecHints, ResultKind, error) {
		cmd := bson.D{{Key: "find", Value: collection}}
		hints := ExecHints{}

		// Add filter (default to {} if not provided)
		if len(args) > 0 && args[0] != nil {
			cmd = append(cmd, bson.E{Key: "filter", Value: args[0]})
		} else {
			cmd = append(cmd, bson.E{Key: "filter", Value: bson.D{}})
		}

		// Add projection if provided
		if len(args) > 1 && args[1] != nil {
			cmd = append(cmd, bson.E{Key: "projection", Value: args[1]})
		}

		// Handle findOne logic
		if onlyOne {
			hints.Limit = intPtr(1)
			hints.SingleBatch = boolPtr(true)
			hints.BatchSize = intPtr(1)
			cmd = append(cmd,
				bson.E{Key: "limit", Value: 1},
				bson.E{Key: "singleBatch", Value: true},
				bson.E{Key: "batchSize", Value: 1},
			)
		}

		return cmd, hints, ResultCursor, nil
	}
}

// AggregateBase creates an aggregate command builder.
func AggregateBase(includeCursor bool) BuildFunc {
	return func(collection string, args []any) (bson.D, ExecHints, ResultKind, error) {
		cmd := bson.D{{Key: "aggregate", Value: collection}}
		hints := ExecHints{}

		if len(args) > 0 && args[0] != nil {
			cmd = append(cmd, bson.E{Key: "pipeline", Value: args[0]})
		} else {
			cmd = append(cmd, bson.E{Key: "pipeline", Value: bson.A{}})
		}

		if includeCursor {
			cmd = append(cmd, bson.E{Key: "cursor", Value: bson.D{}})
		}

		return cmd, hints, ResultCursor, nil
	}
}

// CollStats creates a $collStats-based aggregation builder.
func CollStats(kind string) BuildFunc {
	return func(collection string, args []any) (bson.D, ExecHints, ResultKind, error) {
		var collStatsStage bson.D

		if kind == "latencyStats" {
			collStatsStage = bson.D{{Key: "latencyStats", Value: bson.D{{Key: "histograms", Value: true}}}}
			if len(args) > 0 && args[0] != nil {
				if opts, ok := args[0].(bson.D); ok {
					collStatsStage = bson.D{{Key: "latencyStats", Value: opts}}
				}
			}
		} else {
			collStatsStage = bson.D{{Key: kind, Value: bson.D{}}}
			if len(args) > 0 && args[0] != nil {
				if opts, ok := args[0].(bson.D); ok {
					collStatsStage = opts
				}
			}
		}

		pipeline := bson.A{bson.D{{Key: "$collStats", Value: collStatsStage}}}
		cmd := bson.D{
			{Key: "aggregate", Value: collection},
			{Key: "pipeline", Value: pipeline},
			{Key: "cursor", Value: bson.D{}},
		}

		return cmd, ExecHints{}, ResultCursor, nil
	}
}

// AggregateCount creates a builder for count aggregation.
func AggregateCount() BuildFunc {
	return func(collection string, args []any) (bson.D, ExecHints, ResultKind, error) {
		pipeline := bson.A{}

		if len(args) > 0 && args[0] != nil {
			pipeline = append(pipeline, bson.D{{Key: "$match", Value: args[0]}})
		}
		pipeline = append(pipeline, bson.D{{Key: "$count", Value: "n"}})

		cmd := bson.D{
			{Key: "aggregate", Value: collection},
			{Key: "pipeline", Value: pipeline},
			{Key: "cursor", Value: bson.D{}},
		}

		return cmd, ExecHints{}, ResultCursor, nil
	}
}

// DistinctBase creates a distinct command builder.
func DistinctBase() BuildFunc {
	return func(collection string, args []any) (bson.D, ExecHints, ResultKind, error) {
		cmd := bson.D{{Key: "distinct", Value: collection}}
		if len(args) > 0 && args[0] != nil {
			cmd = append(cmd, bson.E{Key: "key", Value: args[0]})
		} else {
			return nil, ExecHints{}, ResultScalar, fmt.Errorf("distinct requires a field name")
		}
		if len(args) > 1 && args[1] != nil {
			cmd = append(cmd, bson.E{Key: "query", Value: args[1]})
		}
		if len(args) > 2 && args[2] != nil {
			if opts, ok := args[2].(bson.D); ok {
				cmd = append(cmd, opts...)
			}
		}
		return cmd, ExecHints{}, ResultScalar, nil
	}
}

// GetIndexesBase creates a listIndexes command builder.
func GetIndexesBase() BuildFunc {
	return func(collection string, args []any) (bson.D, ExecHints, ResultKind, error) {
		return bson.D{
			{Key: "listIndexes", Value: collection},
			{Key: "cursor", Value: bson.D{}},
		}, ExecHints{}, ResultCursor, nil
	}
}

// EstimatedDocumentCountBase creates a count command builder.
func EstimatedDocumentCountBase() BuildFunc {
	return func(collection string, args []any) (bson.D, ExecHints, ResultKind, error) {
		return bson.D{{Key: "count", Value: collection}}, ExecHints{}, ResultScalar, nil
	}
}

// PassCommand creates a pass-through command builder for db.runCommand()
func PassCommand() BuildFunc {
	return func(collection string, args []any) (bson.D, ExecHints, ResultKind, error) {
		if len(args) == 0 || args[0] == nil {
			return nil, ExecHints{}, ResultScalar, fmt.Errorf("runCommand requires a command document")
		}
		cmd, ok := args[0].(bson.D)
		if !ok {
			return nil, ExecHints{}, ResultScalar, fmt.Errorf("runCommand requires a command document")
		}
		return cmd, ExecHints{}, ResultScalar, nil
	}
}

// WrapExplain wraps a command in an explain envelope.
func WrapExplain(cmd bson.D, verbosity string) bson.D {
	result := bson.D{{Key: "explain", Value: cmd}}
	if verbosity != "" && verbosity != "queryPlanner" {
		result = append(result, bson.E{Key: "verbosity", Value: verbosity})
	}
	return result
}

// Upsert updates or inserts a key-value pair in a bson.D document.
func Upsert(doc bson.D, key string, value interface{}) bson.D {
	for i := range doc {
		if doc[i].Key == key {
			doc[i].Value = value
			return doc
		}
	}
	return append(doc, bson.E{Key: key, Value: value})
}

// extractField extracts a field value from a bson.D document.
func extractField(doc bson.D, key string) (interface{}, bool) {
	for _, elem := range doc {
		if elem.Key == key {
			return elem.Value, true
		}
	}
	return nil, false
}

// intPtr returns a pointer to an int value.
func intPtr(v int) *int {
	return &v
}

// int64Ptr returns a pointer to an int64 value.
func int64Ptr(v int64) *int64 {
	return &v
}

// boolPtr returns a pointer to a bool value.
func boolPtr(v bool) *bool {
	return &v
}
