// Package command provides a table-driven registry for MongoDB shell commands
// and chainers, replacing switch-heavy builder and validator logic.
package command

import (
	"go.mongodb.org/mongo-driver/v2/bson"
)

// ArgKind represents the type of argument expected by a method or chainer.
type ArgKind int

const (
	ArgJSON ArgKind = iota // object/array JSON region
	ArgString              // string argument
	ArgInt                 // integer argument
	ArgBool                // boolean argument
	ArgOptional            // marks an optional argument
)

// ResultKind indicates whether a command returns a cursor or a scalar value.
type ResultKind int

const (
	ResultScalar ResultKind = iota // command returns a single value
	ResultCursor                   // command returns a cursor
)

func (r ResultKind) String() string {
	switch r {
	case ResultScalar:
		return "Scalar"
	case ResultCursor:
		return "Cursor"
	default:
		return "Unknown"
	}
}

// ExecHints provides optional execution hints derived from shell sugar.
type ExecHints struct {
	Limit        *int   // from .limit(n)
	BatchSize    *int   // from .batchSize(n) or implied by limit
	SingleBatch  *bool  // from .singleBatch or policy for find
	MaxTimeMS    *int64 // from .maxTimeMS(n)
	AllowDiskUse *bool  // from .allowDiskUse(bool)
}

// ChainerSpec defines how a chainer modifies a command.
type ChainerSpec struct {
	Name  string    // Chainer name
	Args  []ArgKind // Expected argument kinds
	Apply func(cmd bson.D, hints *ExecHints, args []any) (bson.D, error)
}

// BuildFunc is the signature for method build functions.
// It takes collection name and parsed arguments, returns command, hints, result kind, and error.
type BuildFunc func(collection string, args []any) (cmd bson.D, hints ExecHints, kind ResultKind, err error)

// MethodSpec defines how a method builds a MongoDB command.
type MethodSpec struct {
	Scope    string                  // "collection" or "database"
	Args     []ArgKind               // Expected argument kinds
	Build    BuildFunc               // Command builder function
	Chainers map[string]ChainerSpec // Allowed chainers for this method
}

// Registry maps method names to their specifications.
var Registry = map[string]MethodSpec{}

func init() {
	Registry = map[string]MethodSpec{
		// Collection methods
		"find":                   findSpec,
		"findone":                findOneSpec,
		"aggregate":              aggregateSpec,
		"distinct":               distinctSpec,
		"getindexes":             getIndexesSpec,
		"stats":                  statsSpec,
		"iscapped":               isCappedSpec,
		"latencystats":           latencyStatsSpec,
		"countdocuments":         countDocumentsSpec,
		"estimateddocumentcount": estimatedDocumentCountSpec,
		// Database methods
		"runcommand": runCommandSpec,
	}
}

var findSpec = MethodSpec{
	Scope: "collection",
	Args:  []ArgKind{ArgJSON, ArgOptional, ArgJSON},
	Build: FindBase(false),
	Chainers: map[string]ChainerSpec{
		"sort":        sortChainer,
		"skip":        skipChainer,
		"limit":       limitChainer,
		"batchsize":   batchSizeChainer,
		"singlebatch": singleBatchChainer,
		"maxtimems":   maxTimeMSChainer,
	},
}

var findOneSpec = MethodSpec{
	Scope: "collection",
	Args:  []ArgKind{ArgJSON, ArgOptional, ArgJSON},
	Build: FindBase(true),
	Chainers: map[string]ChainerSpec{
		"maxtimems": maxTimeMSChainer,
	},
}

var aggregateSpec = MethodSpec{
	Scope: "collection",
	Args:  []ArgKind{ArgJSON},
	Build: AggregateBase(true),
	Chainers: map[string]ChainerSpec{
		"allowdiskuse": allowDiskUseChainer,
		"maxtimems":    maxTimeMSChainer,
		"batchsize":    batchSizeChainer,
	},
}

var distinctSpec = MethodSpec{
	Scope: "collection",
	Args:  []ArgKind{ArgString, ArgOptional, ArgJSON, ArgOptional, ArgJSON},
	Build: DistinctBase(),
	Chainers: map[string]ChainerSpec{
		"maxtimems": maxTimeMSChainer,
	},
}

var getIndexesSpec = MethodSpec{
	Scope:    "collection",
	Args:     []ArgKind{},
	Build:    GetIndexesBase(),
	Chainers: map[string]ChainerSpec{},
}

var statsSpec = MethodSpec{
	Scope:    "collection",
	Args:     []ArgKind{ArgOptional, ArgJSON},
	Build:    CollStats("storageStats"),
	Chainers: map[string]ChainerSpec{},
}

var isCappedSpec = MethodSpec{
	Scope:    "collection",
	Args:     []ArgKind{},
	Build:    CollStats("storageStats"),
	Chainers: map[string]ChainerSpec{},
}

var latencyStatsSpec = MethodSpec{
	Scope:    "collection",
	Args:     []ArgKind{ArgOptional, ArgJSON},
	Build:    CollStats("latencyStats"),
	Chainers: map[string]ChainerSpec{},
}

var countDocumentsSpec = MethodSpec{
	Scope: "collection",
	Args:  []ArgKind{ArgJSON},
	Build: AggregateCount(),
	Chainers: map[string]ChainerSpec{
		"maxtimems": maxTimeMSChainer,
	},
}

var estimatedDocumentCountSpec = MethodSpec{
	Scope:    "collection",
	Args:     []ArgKind{},
	Build:    EstimatedDocumentCountBase(),
	Chainers: map[string]ChainerSpec{
		"maxtimems": maxTimeMSChainer,
	},
}

var runCommandSpec = MethodSpec{
	Scope:    "database",
	Args:     []ArgKind{ArgJSON},
	Build:    PassCommand(),
	Chainers: map[string]ChainerSpec{},
}
