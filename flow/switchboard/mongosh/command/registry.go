// Package command provides a table-driven registry for MongoDB shell commands
// and chainers, replacing switch-heavy builder and validator logic.
package command

import (
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// ArgKind represents the type of argument expected by a method or chainer.
type ArgKind int

const (
	ArgJSON   ArgKind = iota // object/array JSON region
	ArgString                // string argument
	ArgInt                   // integer argument
	ArgBool                  // boolean argument
)

// ArgSpec describes a single argument with its type, display name, and optionality.
type ArgSpec struct {
	Name     string
	Kind     ArgKind
	Optional bool
}

// Scope indicates whether a method operates on a collection or a database.
type Scope int

const (
	ScopeCollection Scope = iota
	ScopeDatabase
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
//
//nolint:govet // fieldalignment: readability preferred
type ChainerSpec struct {
	Name  string    // Chainer name
	Args  []ArgSpec // Expected arguments
	Apply func(cmd bson.D, hints *ExecHints, args []any) (bson.D, error)
}

// BuildFunc is the signature for method build functions.
// It takes collection name and parsed arguments, returns command, hints, result kind, and error.
type BuildFunc func(collection string, args []any) (cmd bson.D, hints ExecHints, kind ResultKind, err error)

// MethodSpec defines how a method builds a MongoDB command.
//
//nolint:govet // fieldalignment: readability preferred
type MethodSpec struct {
	Name     string                 // Display name (e.g., "findOne" not "findone")
	Scope    Scope                  // ScopeCollection or ScopeDatabase
	Args     []ArgSpec              // Expected arguments
	Build    BuildFunc              // Command builder function
	Chainers map[string]ChainerSpec // Allowed chainers for this method (lowercase keys)
}

// methodSpecs is the flat list of method definitions; init() builds Registry from it.
var methodSpecs = []MethodSpec{
	findSpec, findOneSpec, aggregateSpec, distinctSpec,
	getIndexesSpec, statsSpec, isCappedSpec, latencyStatsSpec,
	countDocumentsSpec, estimatedDocumentCountSpec,
	runCommandSpec,
}

// Registry maps lowercase method names to their specifications.
var Registry map[string]MethodSpec

func init() {
	Registry = make(map[string]MethodSpec, len(methodSpecs))
	for _, spec := range methodSpecs {
		Registry[strings.ToLower(spec.Name)] = spec
	}
}

var findSpec = MethodSpec{
	Name:  "find",
	Scope: ScopeCollection,
	Args:  []ArgSpec{{"filter", ArgJSON, false}, {"projection", ArgJSON, true}},
	Build: FindBase(false),
	Chainers: chainers(
		sortChainer,
		skipChainer,
		limitChainer,
		batchSizeChainer,
		singleBatchChainer,
		maxTimeMSChainer,
	),
}

var findOneSpec = MethodSpec{
	Name:     "findOne",
	Scope:    ScopeCollection,
	Args:     []ArgSpec{{"filter", ArgJSON, false}, {"projection", ArgJSON, true}},
	Build:    FindBase(true),
	Chainers: chainers(maxTimeMSChainer),
}

var aggregateSpec = MethodSpec{
	Name:  "aggregate",
	Scope: ScopeCollection,
	Args:  []ArgSpec{{"pipeline", ArgJSON, false}},
	Build: AggregateBase(true),
	Chainers: chainers(
		allowDiskUseChainer,
		maxTimeMSChainer,
		batchSizeChainer,
	),
}

var distinctSpec = MethodSpec{
	Name:     "distinct",
	Scope:    ScopeCollection,
	Args:     []ArgSpec{{"field", ArgString, false}, {"query", ArgJSON, true}, {"options", ArgJSON, true}},
	Build:    DistinctBase(),
	Chainers: chainers(maxTimeMSChainer),
}

var getIndexesSpec = MethodSpec{
	Name:  "getIndexes",
	Scope: ScopeCollection,
	Args:  nil,
	Build: GetIndexesBase(),
}

var statsSpec = MethodSpec{
	Name:  "stats",
	Scope: ScopeCollection,
	Args:  []ArgSpec{{"options", ArgJSON, true}},
	Build: CollStats("storageStats"),
}

var isCappedSpec = MethodSpec{
	Name:  "isCapped",
	Scope: ScopeCollection,
	Args:  nil,
	Build: CollStats("storageStats"),
}

var latencyStatsSpec = MethodSpec{
	Name:  "latencyStats",
	Scope: ScopeCollection,
	Args:  []ArgSpec{{"options", ArgJSON, true}},
	Build: CollStats("latencyStats"),
}

var countDocumentsSpec = MethodSpec{
	Name:     "countDocuments",
	Scope:    ScopeCollection,
	Args:     []ArgSpec{{"filter", ArgJSON, false}},
	Build:    AggregateCount(),
	Chainers: chainers(maxTimeMSChainer),
}

var estimatedDocumentCountSpec = MethodSpec{
	Name:     "estimatedDocumentCount",
	Scope:    ScopeCollection,
	Args:     nil,
	Build:    EstimatedDocumentCountBase(),
	Chainers: chainers(maxTimeMSChainer),
}

var runCommandSpec = MethodSpec{
	Name:  "runCommand",
	Scope: ScopeDatabase,
	Args:  []ArgSpec{{"command", ArgJSON, false}},
	Build: PassCommand(),
}

// chainers builds a map[lowercase_name]ChainerSpec from a list of specs.
func chainers(specs ...ChainerSpec) map[string]ChainerSpec {
	m := make(map[string]ChainerSpec, len(specs))
	for _, s := range specs {
		m[strings.ToLower(s.Name)] = s
	}
	return m
}
