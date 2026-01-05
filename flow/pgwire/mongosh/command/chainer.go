package command

import (
	"errors"

	"go.mongodb.org/mongo-driver/v2/bson"
)

var sortChainer = ChainerSpec{
	Name: "sort",
	Args: []ArgKind{ArgJSON},
	Apply: func(cmd bson.D, hints *ExecHints, args []any) (bson.D, error) {
		if len(args) < 1 || args[0] == nil {
			return cmd, nil
		}
		return Upsert(cmd, "sort", args[0]), nil
	},
}

var skipChainer = ChainerSpec{
	Name: "skip",
	Args: []ArgKind{ArgInt},
	Apply: func(cmd bson.D, hints *ExecHints, args []any) (bson.D, error) {
		if len(args) < 1 || args[0] == nil {
			return cmd, nil
		}
		skip, ok := args[0].(int)
		if !ok {
			return nil, errors.New("skip must be an integer")
		}
		return Upsert(cmd, "skip", skip), nil
	},
}

var limitChainer = ChainerSpec{
	Name: "limit",
	Args: []ArgKind{ArgInt},
	Apply: func(cmd bson.D, hints *ExecHints, args []any) (bson.D, error) {
		if len(args) < 1 || args[0] == nil {
			return cmd, nil
		}
		limit, ok := args[0].(int)
		if !ok {
			return nil, errors.New("limit must be an integer")
		}
		hints.Limit = intPtr(limit)
		cmd = Upsert(cmd, "limit", limit)

		// For find commands, also set batchSize and singleBatch
		if cmdName, ok := extractField(cmd, "find"); ok && cmdName != nil {
			if limit > 0 {
				hints.BatchSize = intPtr(limit)
				hints.SingleBatch = boolPtr(true)
				cmd = Upsert(cmd, "singleBatch", true)
				cmd = Upsert(cmd, "batchSize", limit)
			}
		}
		return cmd, nil
	},
}

var batchSizeChainer = ChainerSpec{
	Name: "batchSize",
	Args: []ArgKind{ArgInt},
	Apply: func(cmd bson.D, hints *ExecHints, args []any) (bson.D, error) {
		if len(args) < 1 || args[0] == nil {
			return cmd, nil
		}
		size, ok := args[0].(int)
		if !ok {
			return nil, errors.New("batchSize must be an integer")
		}
		hints.BatchSize = intPtr(size)

		// For aggregate, update cursor.batchSize
		if _, ok := extractField(cmd, "aggregate"); ok {
			if cursor, ok := extractField(cmd, "cursor"); ok {
				if cursorDoc, ok := cursor.(bson.D); ok {
					cursorDoc = Upsert(cursorDoc, "batchSize", size)
					cmd = Upsert(cmd, "cursor", cursorDoc)
				}
			}
		} else {
			cmd = Upsert(cmd, "batchSize", size)
		}
		return cmd, nil
	},
}

var singleBatchChainer = ChainerSpec{
	Name: "singleBatch",
	Args: []ArgKind{ArgBool},
	Apply: func(cmd bson.D, hints *ExecHints, args []any) (bson.D, error) {
		if len(args) < 1 || args[0] == nil {
			return cmd, nil
		}
		single, ok := args[0].(bool)
		if !ok {
			return nil, errors.New("singleBatch must be a boolean")
		}
		hints.SingleBatch = boolPtr(single)
		return Upsert(cmd, "singleBatch", single), nil
	},
}

var maxTimeMSChainer = ChainerSpec{
	Name: "maxTimeMS",
	Args: []ArgKind{ArgInt},
	Apply: func(cmd bson.D, hints *ExecHints, args []any) (bson.D, error) {
		if len(args) < 1 || args[0] == nil {
			return cmd, nil
		}
		msInt, ok := args[0].(int)
		if !ok {
			return nil, errors.New("maxTimeMS must be an integer")
		}
		ms := int64(msInt)
		hints.MaxTimeMS = int64Ptr(ms)
		return Upsert(cmd, "maxTimeMS", ms), nil
	},
}

var allowDiskUseChainer = ChainerSpec{
	Name: "allowDiskUse",
	Args: []ArgKind{ArgBool},
	Apply: func(cmd bson.D, hints *ExecHints, args []any) (bson.D, error) {
		if len(args) < 1 || args[0] == nil {
			return cmd, nil
		}
		allow, ok := args[0].(bool)
		if !ok {
			return nil, errors.New("allowDiskUse must be a boolean")
		}
		hints.AllowDiskUse = boolPtr(allow)
		return Upsert(cmd, "allowDiskUse", allow), nil
	},
}
