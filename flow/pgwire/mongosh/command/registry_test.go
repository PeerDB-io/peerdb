package command

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestRegistry(t *testing.T) {
	// Test that registry is populated
	require.NotEmpty(t, Registry)

	// Test key methods exist
	expectedMethods := []string{
		"find", "findone", "aggregate", "distinct",
		"getindexes", "stats", "iscapped", "latencystats",
		"countdocuments", "estimateddocumentcount", "runcommand",
	}

	for _, method := range expectedMethods {
		_, ok := Registry[method]
		require.True(t, ok, "Registry missing method: %s", method)
	}
}

func TestFindSpec(t *testing.T) {
	spec, ok := Registry["find"]
	require.True(t, ok, "find not in registry")

	// Test chainers
	expectedChainers := []string{"sort", "skip", "limit", "batchsize", "singlebatch", "maxtimems"}
	for _, chainer := range expectedChainers {
		_, ok := spec.Chainers[chainer]
		require.True(t, ok, "find missing chainer: %s", chainer)
	}

	// Test build function
	args, err := ParseArgsAccordingTo(spec.Args, []string{`{"name": "test"}`})
	require.NoError(t, err, "ParseArgsAccordingTo failed")

	cmd, _, kind, err := spec.Build("users", args)
	require.NoError(t, err, "Build failed")

	require.Equal(t, ResultCursor, kind)

	// Check command structure
	require.GreaterOrEqual(t, len(cmd), 2, "Command too short")
	require.Equal(t, "find", cmd[0].Key)
	require.Equal(t, "users", cmd[0].Value)

	// Test limit chainer
	limitChainer := spec.Chainers["limit"]
	testHints := ExecHints{}
	_, err = limitChainer.Apply(cmd, &testHints, []any{10})
	require.NoError(t, err, "limit chainer failed")
	require.NotNil(t, testHints.Limit)
	require.EqualValues(t, 10, *testHints.Limit, "limit not set in hints")
}

func TestAggregateSpec(t *testing.T) {
	spec, ok := Registry["aggregate"]
	require.True(t, ok, "aggregate not in registry")

	args, err := ParseArgsAccordingTo(spec.Args, []string{`[{"$match": {"status": "active"}}]`})
	require.NoError(t, err, "ParseArgsAccordingTo failed")

	cmd, _, kind, err := spec.Build("orders", args)
	require.NoError(t, err, "Build failed")

	require.Equal(t, ResultCursor, kind)

	// Check for cursor field
	hasCursor := false
	for _, elem := range cmd {
		if elem.Key == "cursor" {
			hasCursor = true
			break
		}
	}
	require.True(t, hasCursor, "aggregate command missing cursor field")
}

func TestDistinctSpec(t *testing.T) {
	spec, ok := Registry["distinct"]
	require.True(t, ok, "distinct not in registry")

	// Test with field only
	args, err := ParseArgsAccordingTo(spec.Args, []string{`"email"`})
	require.NoError(t, err, "ParseArgsAccordingTo failed")

	cmd, _, kind, err := spec.Build("users", args)
	require.NoError(t, err, "Build failed")

	require.Equal(t, ResultScalar, kind)

	// Check key field
	hasKey := false
	for _, elem := range cmd {
		if elem.Key == "key" && elem.Value == "email" {
			hasKey = true
			break
		}
	}
	require.True(t, hasKey, "distinct command missing key field")

	// Test with field and query
	args, err = ParseArgsAccordingTo(spec.Args, []string{`"email"`, `{"active": true}`})
	require.NoError(t, err, "ParseArgsAccordingTo with query failed")

	cmd, _, _, err = spec.Build("users", args)
	require.NoError(t, err, "Build with query failed")

	hasQuery := false
	for _, elem := range cmd {
		if elem.Key == "query" {
			hasQuery = true
			break
		}
	}
	require.True(t, hasQuery, "distinct command missing query field")
}

func TestRunCommandSpec(t *testing.T) {
	spec, ok := Registry["runcommand"]
	require.True(t, ok, "runcommand not in registry")

	args, err := ParseArgsAccordingTo(spec.Args, []string{`{"ping": 1}`})
	require.NoError(t, err, "ParseArgsAccordingTo failed")

	cmd, _, kind, err := spec.Build("", args)
	require.NoError(t, err, "Build failed")

	require.Equal(t, ResultScalar, kind)

	// runCommand should return the command as-is
	require.NotEmpty(t, cmd, "runCommand returned empty command")
}

func TestChainerApplication(t *testing.T) {
	// Test sort chainer
	sortChainer := Registry["find"].Chainers["sort"]
	cmd := bson.D{{Key: "find", Value: "users"}}
	hints := ExecHints{}

	sortDoc := map[string]interface{}{"name": 1}
	cmd, err := sortChainer.Apply(cmd, &hints, []any{sortDoc})
	require.NoError(t, err, "sort chainer failed")

	// Check sort was added
	hasSort := false
	for _, elem := range cmd {
		if elem.Key == "sort" {
			hasSort = true
			break
		}
	}
	require.True(t, hasSort, "sort not added to command")

	// Test maxTimeMS chainer
	maxTimeChainer := Registry["find"].Chainers["maxtimems"]
	cmd, err = maxTimeChainer.Apply(cmd, &hints, []any{5000})
	require.NoError(t, err, "maxTimeMS chainer failed")

	require.NotNil(t, hints.MaxTimeMS)
	require.Equal(t, int64(5000), *hints.MaxTimeMS, "maxTimeMS not set in hints")

	// Check maxTimeMS was added to command
	hasMaxTime := false
	for _, elem := range cmd {
		if elem.Key == "maxTimeMS" && elem.Value == int64(5000) {
			hasMaxTime = true
			break
		}
	}
	require.True(t, hasMaxTime, "maxTimeMS not added to command")
}
