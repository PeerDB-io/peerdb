package mongosh

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// TestCompile_Queries tests compilation of mongosh queries.
//
//nolint:govet // fieldalignment: test code, readability preferred
func TestCompile_Queries(t *testing.T) {
	type wantError struct {
		Kind ErrorKind
		Msg  string // substring match
	}

	intPtr := func(i int) *int { return &i }
	int64Ptr := func(i int64) *int64 { return &i }
	boolPtr := func(b bool) *bool { return &b }
	mustObjectID := func(s string) bson.ObjectID {
		oid, err := bson.ObjectIDFromHex(s)
		if err != nil {
			panic(err)
		}
		return oid
	}

	tests := []struct {
		name      string
		input     string
		wantCmd   bson.D
		wantKind  ResultKind
		wantNS    Namespace
		wantHints ExecHints
		wantErr   *wantError
		wantHelp  bool
	}{
		// ═══════════════════════════════════════════════════════════════════════
		// SUCCESS CASES
		// ═══════════════════════════════════════════════════════════════════════

		// --- find ---
		{
			name:  "find/withQuery",
			input: "db.coll.find({a: {$gt: 1}});",
			wantCmd: bson.D{
				{Key: "find", Value: "coll"},
				{Key: "filter", Value: bson.D{{Key: "a", Value: bson.D{{Key: "$gt", Value: int32(1)}}}}},
			},
			wantKind: ResultCursor,
			wantNS:   Namespace{DB: "test", Collection: "coll"},
		},
		{
			name:  "find/withProjection",
			input: "db.coll.find({}, {_id: 0});",
			wantCmd: bson.D{
				{Key: "find", Value: "coll"},
				{Key: "filter", Value: bson.D{}},
				{Key: "projection", Value: bson.D{{Key: "_id", Value: int32(0)}}},
			},
			wantKind: ResultCursor,
			wantNS:   Namespace{DB: "test", Collection: "coll"},
		},
		{
			name:  "find/withFilterAndProjection",
			input: `db.users.find({"active": true}, {"email": 1, "name": 1})`,
			wantCmd: bson.D{
				{Key: "find", Value: "users"},
				{Key: "filter", Value: bson.D{{Key: "active", Value: true}}},
				{Key: "projection", Value: bson.D{
					{Key: "email", Value: int32(1)},
					{Key: "name", Value: int32(1)},
				}},
			},
			wantKind: ResultCursor,
			wantNS:   Namespace{DB: "test", Collection: "users"},
		},
		{
			name:  "find/withRegex",
			input: "db.coll.find({a: /cat/});",
			wantCmd: bson.D{
				{Key: "find", Value: "coll"},
				{Key: "filter", Value: bson.D{
					{Key: "a", Value: bson.Regex{Pattern: "cat", Options: ""}},
				}},
			},
			wantKind: ResultCursor,
			wantNS:   Namespace{DB: "test", Collection: "coll"},
		},
		{
			name:  "find/withRegexCaseInsensitive",
			input: "db.coll.find({a: /Cat/i});",
			wantCmd: bson.D{
				{Key: "find", Value: "coll"},
				{Key: "filter", Value: bson.D{
					{Key: "a", Value: bson.Regex{Pattern: "Cat", Options: "i"}},
				}},
			},
			wantKind: ResultCursor,
			wantNS:   Namespace{DB: "test", Collection: "coll"},
		},
		{
			name:  "find/withObjectId",
			input: `db.users.find({"_id": ObjectId("6a8b2c3d4e5f67890abcdef1")})`,
			wantCmd: bson.D{
				{Key: "find", Value: "users"},
				{Key: "filter", Value: bson.D{
					{Key: "_id", Value: mustObjectID("6a8b2c3d4e5f67890abcdef1")},
				}},
			},
			wantKind: ResultCursor,
			wantNS:   Namespace{DB: "test", Collection: "users"},
		},
		{
			name:  "find/withISODate",
			input: `db.events.find({"created": ISODate("2023-06-15T12:30:45Z")})`,
			wantCmd: bson.D{
				{Key: "find", Value: "events"},
				{Key: "filter", Value: bson.D{
					{Key: "created", Value: bson.NewDateTimeFromTime(time.Date(2023, 6, 15, 12, 30, 45, 0, time.UTC))},
				}},
			},
			wantKind: ResultCursor,
			wantNS:   Namespace{DB: "test", Collection: "events"},
		},
		{
			name:  "find/withSort",
			input: `db.users.find({}).sort({"name": 1})`,
			wantCmd: bson.D{
				{Key: "find", Value: "users"},
				{Key: "filter", Value: bson.D{}},
				{Key: "sort", Value: bson.D{{Key: "name", Value: int32(1)}}},
			},
			wantKind: ResultCursor,
			wantNS:   Namespace{DB: "test", Collection: "users"},
		},
		{
			name:  "find/withSortAndLimit",
			input: `db.users.find({}).sort({"name": 1}).limit(10)`,
			wantCmd: bson.D{
				{Key: "find", Value: "users"},
				{Key: "filter", Value: bson.D{}},
				{Key: "sort", Value: bson.D{{Key: "name", Value: int32(1)}}},
				{Key: "limit", Value: 10},
				{Key: "singleBatch", Value: true},
				{Key: "batchSize", Value: 10},
			},
			wantKind:  ResultCursor,
			wantNS:    Namespace{DB: "test", Collection: "users"},
			wantHints: ExecHints{Limit: intPtr(10), SingleBatch: boolPtr(true), BatchSize: intPtr(10)},
		},
		{
			name:  "find/withSkipAndLimit",
			input: `db.users.find({}).skip(20).limit(10)`,
			wantCmd: bson.D{
				{Key: "find", Value: "users"},
				{Key: "filter", Value: bson.D{}},
				{Key: "skip", Value: 20},
				{Key: "limit", Value: 10},
				{Key: "singleBatch", Value: true},
				{Key: "batchSize", Value: 10},
			},
			wantKind:  ResultCursor,
			wantNS:    Namespace{DB: "test", Collection: "users"},
			wantHints: ExecHints{Limit: intPtr(10), SingleBatch: boolPtr(true), BatchSize: intPtr(10)},
		},
		{
			name:  "find/withBatchSize",
			input: `db.coll.find({a: "a"}).batchSize(10);`,
			wantCmd: bson.D{
				{Key: "find", Value: "coll"},
				{Key: "filter", Value: bson.D{{Key: "a", Value: "a"}}},
				{Key: "batchSize", Value: 10},
			},
			wantKind:  ResultCursor,
			wantNS:    Namespace{DB: "test", Collection: "coll"},
			wantHints: ExecHints{BatchSize: intPtr(10)},
		},
		{
			name:  "find/withMaxTimeMS",
			input: `db.users.find({}).maxTimeMS(7500)`,
			wantCmd: bson.D{
				{Key: "find", Value: "users"},
				{Key: "filter", Value: bson.D{}},
				{Key: "maxTimeMS", Value: int64(7500)},
			},
			wantKind:  ResultCursor,
			wantNS:    Namespace{DB: "test", Collection: "users"},
			wantHints: ExecHints{MaxTimeMS: int64Ptr(7500)},
		},

		// --- findOne ---
		{
			name:  "findOne/basic",
			input: `db.coll.findOne({"v": "a"});`,
			wantCmd: bson.D{
				{Key: "find", Value: "coll"},
				{Key: "filter", Value: bson.D{{Key: "v", Value: "a"}}},
				{Key: "limit", Value: 1},
				{Key: "singleBatch", Value: true},
				{Key: "batchSize", Value: 1},
			},
			wantKind:  ResultCursor,
			wantNS:    Namespace{DB: "test", Collection: "coll"},
			wantHints: ExecHints{Limit: intPtr(1), SingleBatch: boolPtr(true), BatchSize: intPtr(1)},
		},
		{
			name: "findOne/withISODate",
			input: `db.coll.findOne({
    "timestamp": new ISODate("2024-07-21T16:22:10.000Z")
}, {_id: 0, timestamp: 1});`,
			wantCmd: bson.D{
				{Key: "find", Value: "coll"},
				{Key: "filter", Value: bson.D{{Key: "timestamp", Value: bson.DateTime(1721578930000)}}},
				{Key: "projection", Value: bson.D{{Key: "_id", Value: int32(0)}, {Key: "timestamp", Value: int32(1)}}},
				{Key: "limit", Value: 1},
				{Key: "singleBatch", Value: true},
				{Key: "batchSize", Value: 1},
			},
			wantKind:  ResultCursor,
			wantNS:    Namespace{DB: "test", Collection: "coll"},
			wantHints: ExecHints{Limit: intPtr(1), SingleBatch: boolPtr(true), BatchSize: intPtr(1)},
		},

		// --- aggregate ---
		{
			name: "aggregate/basic",
			input: `db.coll.aggregate([
    { $match: { status: "A" } },
    { $group: { _id: "$group", total: { $sum: "$amount" } } },
    { $sort: { total: -1 } }
]);`,
			wantCmd: bson.D{
				{Key: "aggregate", Value: "coll"},
				{Key: "pipeline", Value: bson.A{
					bson.D{{Key: "$match", Value: bson.D{{Key: "status", Value: "A"}}}},
					bson.D{{Key: "$group", Value: bson.D{
						{Key: "_id", Value: "$group"},
						{Key: "total", Value: bson.D{{Key: "$sum", Value: "$amount"}}},
					}}},
					bson.D{{Key: "$sort", Value: bson.D{{Key: "total", Value: int32(-1)}}}},
				}},
				{Key: "cursor", Value: bson.D{}},
			},
			wantKind: ResultCursor,
			wantNS:   Namespace{DB: "test", Collection: "coll"},
		},
		{
			name:  "aggregate/withAllowDiskUse",
			input: `db.orders.aggregate([{"$group": {"_id": "$status", "count": {"$sum": 1}}}]).allowDiskUse(true)`,
			wantCmd: bson.D{
				{Key: "aggregate", Value: "orders"},
				{Key: "pipeline", Value: bson.A{
					bson.D{{Key: "$group", Value: bson.D{
						{Key: "_id", Value: "$status"},
						{Key: "count", Value: bson.D{{Key: "$sum", Value: int32(1)}}},
					}}},
				}},
				{Key: "cursor", Value: bson.D{}},
				{Key: "allowDiskUse", Value: true},
			},
			wantKind:  ResultCursor,
			wantNS:    Namespace{DB: "test", Collection: "orders"},
			wantHints: ExecHints{AllowDiskUse: boolPtr(true)},
		},

		// --- distinct ---
		{
			name:  "distinct/fieldOnly",
			input: `db.users.distinct("email")`,
			wantCmd: bson.D{
				{Key: "distinct", Value: "users"},
				{Key: "key", Value: "email"},
			},
			wantKind: ResultScalar,
			wantNS:   Namespace{DB: "test", Collection: "users"},
		},
		{
			name:  "distinct/withQuery",
			input: `db.users.distinct("email", {"active": true})`,
			wantCmd: bson.D{
				{Key: "distinct", Value: "users"},
				{Key: "key", Value: "email"},
				{Key: "query", Value: bson.D{{Key: "active", Value: true}}},
			},
			wantKind: ResultScalar,
			wantNS:   Namespace{DB: "test", Collection: "users"},
		},
		{
			name:  "distinct/withCollation",
			input: `db.coll.distinct("category", {}, {collation: {locale: "fr", strength: 1}});`,
			wantCmd: bson.D{
				{Key: "distinct", Value: "coll"},
				{Key: "key", Value: "category"},
				{Key: "query", Value: bson.D{}},
				{Key: "collation", Value: bson.D{{Key: "locale", Value: "fr"}, {Key: "strength", Value: int32(1)}}},
			},
			wantKind: ResultScalar,
			wantNS:   Namespace{DB: "test", Collection: "coll"},
		},

		// --- estimatedDocumentCount ---
		{
			name:     "estimatedDocumentCount/basic",
			input:    "db.coll.estimatedDocumentCount();",
			wantCmd:  bson.D{{Key: "count", Value: "coll"}},
			wantKind: ResultScalar,
			wantNS:   Namespace{DB: "test", Collection: "coll"},
		},

		// --- getIndexes ---
		{
			name:  "getIndexes/basic",
			input: "db.coll.getIndexes();",
			wantCmd: bson.D{
				{Key: "listIndexes", Value: "coll"},
				{Key: "cursor", Value: bson.D{}},
			},
			wantKind: ResultCursor,
			wantNS:   Namespace{DB: "test", Collection: "coll"},
		},

		// --- stats ---
		{
			name:  "stats/basic",
			input: "db.coll.stats()",
			wantCmd: bson.D{
				{Key: "aggregate", Value: "coll"},
				{Key: "pipeline", Value: bson.A{
					bson.D{{Key: "$collStats", Value: bson.D{{Key: "storageStats", Value: bson.D{}}}}},
				}},
				{Key: "cursor", Value: bson.D{}},
			},
			wantKind: ResultCursor,
			wantNS:   Namespace{DB: "test", Collection: "coll"},
		},

		// --- explain ---
		{
			name:  "explain/findDefault",
			input: `db.users.explain().find({name: "test"})`,
			wantCmd: bson.D{
				{Key: "explain", Value: bson.D{
					{Key: "find", Value: "users"},
					{Key: "filter", Value: bson.D{{Key: "name", Value: "test"}}},
				}},
			},
			wantKind: ResultScalar,
			wantNS:   Namespace{DB: "test", Collection: "users"},
		},
		{
			name:  "explain/findWithVerbosity",
			input: `db.users.explain("executionStats").find({})`,
			wantCmd: bson.D{
				{Key: "explain", Value: bson.D{
					{Key: "find", Value: "users"},
					{Key: "filter", Value: bson.D{}},
				}},
				{Key: "verbosity", Value: "executionStats"},
			},
			wantKind: ResultScalar,
			wantNS:   Namespace{DB: "test", Collection: "users"},
		},
		{
			name:  "explain/aggregate",
			input: `db.orders.explain().aggregate([{$match: {status: "A"}}])`,
			wantCmd: bson.D{
				{Key: "explain", Value: bson.D{
					{Key: "aggregate", Value: "orders"},
					{Key: "pipeline", Value: bson.A{
						bson.D{{Key: "$match", Value: bson.D{{Key: "status", Value: "A"}}}},
					}},
					{Key: "cursor", Value: bson.D{}},
				}},
			},
			wantKind: ResultScalar,
			wantNS:   Namespace{DB: "test", Collection: "orders"},
		},
		{
			name:  "explain/withChainers",
			input: `db.users.explain("allPlansExecution").find({}).sort({name: 1}).limit(10)`,
			wantCmd: bson.D{
				{Key: "explain", Value: bson.D{
					{Key: "find", Value: "users"},
					{Key: "filter", Value: bson.D{}},
					{Key: "sort", Value: bson.D{{Key: "name", Value: int32(1)}}},
					{Key: "limit", Value: 10},
					{Key: "singleBatch", Value: true},
					{Key: "batchSize", Value: 10},
				}},
				{Key: "verbosity", Value: "allPlansExecution"},
			},
			wantKind:  ResultScalar,
			wantNS:    Namespace{DB: "test", Collection: "users"},
			wantHints: ExecHints{Limit: intPtr(10), SingleBatch: boolPtr(true), BatchSize: intPtr(10)},
		},

		// --- runCommand ---
		{
			name:  "runCommand/ping",
			input: `db.runCommand({"ping": 1})`,
			wantCmd: bson.D{
				{Key: "ping", Value: int32(1)},
			},
			wantKind: ResultScalar,
			wantNS:   Namespace{DB: "test", Collection: ""},
		},
		{
			name:     "runCommand/usersInfo",
			input:    "db.runCommand({usersInfo: 'admin'});",
			wantCmd:  bson.D{{Key: "usersInfo", Value: "admin"}},
			wantKind: ResultScalar,
			wantNS:   Namespace{DB: "test", Collection: ""},
		},

		// --- show ---
		{
			name:  "show/collections",
			input: "show collections;",
			wantCmd: bson.D{
				{Key: "listCollections", Value: 1},
				{Key: "nameOnly", Value: true},
				{Key: "authorizedCollections", Value: true},
			},
			wantKind: ResultCursor,
			wantNS:   Namespace{DB: "test"},
		},
		{
			name:  "show/databases",
			input: "show databases",
			wantCmd: bson.D{
				{Key: "listDatabases", Value: 1},
				{Key: "nameOnly", Value: true},
				{Key: "authorizedDatabases", Value: true},
			},
			wantKind: ResultScalar,
			wantNS:   Namespace{DB: "admin"},
		},
		{
			name:  "show/dbs",
			input: "show dbs",
			wantCmd: bson.D{
				{Key: "listDatabases", Value: 1},
				{Key: "nameOnly", Value: true},
				{Key: "authorizedDatabases", Value: true},
			},
			wantKind: ResultScalar,
			wantNS:   Namespace{DB: "admin"},
		},

		// ═══════════════════════════════════════════════════════════════════════
		// PARSE ERRORS
		// ═══════════════════════════════════════════════════════════════════════

		// --- parse (syntax/structure errors) ---
		{
			name:    "parse/emptyInput",
			input:   ``,
			wantErr: &wantError{ErrParse, ""},
		},
		{
			name:    "parse/commentOnly",
			input:   "//db.coll.find({a: \"a\"})",
			wantErr: &wantError{ErrParse, "expected exactly one statement"},
		},
		{
			name:    "parse/dbRefOnly",
			input:   "db",
			wantErr: &wantError{ErrParse, "expected method call"},
		},
		{
			name:    "parse/collectionRefOnly",
			input:   "db.collection",
			wantErr: &wantError{ErrParse, "expected method call"},
		},
		{
			name:    "parse/collectionWithDots",
			input:   "db.coll.with.dot.find();",
			wantErr: &wantError{ErrParse, "invalid statement structure"},
		},
		{
			name:    "parse/missingDbPrefix",
			input:   `users.find({})`,
			wantErr: &wantError{ErrParse, "must start with 'db'"},
		},
		{
			name:    "parse/cursorVariable",
			input:   "cursor.isClosed();",
			wantErr: &wantError{ErrParse, "must start with 'db'"},
		},
		{
			name:     "help/global",
			input:    "help()",
			wantHelp: true,
		},
		{
			name:    "parse/assignment",
			input:   "isCapped = db.coll.isCapped()",
			wantErr: &wantError{ErrParse, "unexpected expression type"},
		},
		{
			name:    "parse/propertyAccess",
			input:   "db.coll.find().next().name;",
			wantErr: &wantError{ErrParse, "unexpected expression type"},
		},
		{
			name:    "parse/arrowFunction",
			input:   "db.coll.find().forEach(d => print(d));",
			wantErr: &wantError{ErrParse, "ArrowFunctionLiteral"},
		},
		{
			name:    "parse/arrowFunctionInMap",
			input:   "db.coll.find().map((u) => u.name);",
			wantErr: &wantError{ErrParse, "ArrowFunctionLiteral"},
		},
		{
			name:    "parse/arrowFunctionAfterFind",
			input:   "db.coll.find().map(d => d.name).batchSize(1);",
			wantErr: &wantError{ErrParse, "ArrowFunctionLiteral"},
		},
		{
			name:    "parse/functionLiteral",
			input:   "db.coll.find().map(function (u) {return u.name;});",
			wantErr: &wantError{ErrParse, "multiple statements"},
		},
		{
			name:    "parse/runCommandWithString",
			input:   "db.runCommand('ismaster')",
			wantErr: &wantError{ErrParse, "runCommand requires a command document"},
		},

		// --- args (argument count errors) ---
		{
			name:    "args/find/missing",
			input:   "db.coll.find();",
			wantErr: &wantError{ErrParse, "expected at least 1 arguments"},
		},
		{
			name:    "args/find/missingOnEmpty",
			input:   "db.emptyCollection.find();",
			wantErr: &wantError{ErrParse, "expected at least 1 arguments"},
		},
		{
			name:    "args/countDocuments/missing",
			input:   "db.coll.countDocuments();",
			wantErr: &wantError{ErrParse, "expected at least 1 arguments"},
		},
		{
			name:    "args/estimatedDocumentCount/tooMany",
			input:   "db.coll.estimatedDocumentCount({maxTimeMS: 1000});",
			wantErr: &wantError{ErrParse, "too many arguments"},
		},
		{
			name:    "args/limit/missingFilter",
			input:   "db.coll.find().limit(2);",
			wantErr: &wantError{ErrParse, "expected at least 1 arguments"},
		},
		{
			name:    "args/skip/missingFilter",
			input:   "db.coll.find().skip(1);",
			wantErr: &wantError{ErrParse, "expected at least 1 arguments"},
		},
		{
			name:    "args/sort/missingFilter",
			input:   "db.coll.find().sort({name: 1});",
			wantErr: &wantError{ErrParse, "expected at least 1 arguments"},
		},
		{
			name:    "args/maxTimeMS/missingFilter",
			input:   "db.coll.find().maxTimeMS(100);",
			wantErr: &wantError{ErrParse, "expected at least 1 arguments"},
		},
		{
			name: "args/aggregate/tooMany",
			input: `db.coll.aggregate([
    {$match: {status: "A"}},
    {$group: {_id: "$group", total: {$sum: "$amount"}}},
    {$sort: {_id: 1}}
], {allowDiskUse: true});`,
			wantErr: &wantError{ErrParse, "too many arguments"},
		},
		{
			name: "args/aggregate/tooManyBatchSize",
			input: `db.coll.aggregate([
    {$match: {status: "A"}},
    {$group: {_id: "$group", total: {$sum: "$amount"}}},
    {$sort: {_id: 1}}
], {cursor: {batchSize: 1}});`,
			wantErr: &wantError{ErrParse, "too many arguments"},
		},
		{
			name: "args/aggregate/tooManyCollation",
			input: `db.coll.aggregate([
    {$match: {status: "A"}},
    {$group: {_id: "$group", total: {$sum: "$amount"}}},
    {$sort: {_id: 1}}
], {collation: {locale: "en", numericOrdering: true}});`,
			wantErr: &wantError{ErrParse, "too many arguments"},
		},
		{
			name: "args/aggregate/tooManyExplain",
			input: `db.coll.aggregate([
    {$match: {status: "A"}},
    {$group: {_id: "$group", total: {$sum: "$amount"}}},
    {$sort: {_id: 1}}
], {explain: true});`,
			wantErr: &wantError{ErrParse, "too many arguments"},
		},
		{
			name: "args/aggregate/tooManyMaxTime",
			input: `db.coll.aggregate([
    {$match: {status: "A"}},
    {$group: {_id: "$group", total: {$sum: "$amount"}}},
    {$sort: {_id: 1}}
], {maxTimeMS: 1000});`,
			wantErr: &wantError{ErrParse, "too many arguments"},
		},
		{
			name: "args/aggregate/tooManyNegBatch",
			input: `db.coll.aggregate([
    {$match: {status: "A"}},
    {$group: {_id: "$group", total: {$sum: "$amount"}}},
    {$sort: {_id: 1}}
], {cursor: {batchSize: -1}});`,
			wantErr: &wantError{ErrParse, "too many arguments"},
		},
		{
			name: "args/aggregate/tooManyReadConcern",
			input: `db.coll.aggregate([
    {$match: {status: "A"}},
    {$group: {_id: "$group", total: {$sum: "$amount"}}},
    {$sort: {total: -1}}
], {readConcern: {level: "local"}});`,
			wantErr: &wantError{ErrParse, "too many arguments"},
		},
		{
			name:    "args/aggregate/notArray",
			input:   `db.coll.aggregate({$match: {status: "A"}}, {$group: {_id: "$group", total: {$sum: "$amount"}}}, {$sort: {total: -1}});`,
			wantErr: &wantError{ErrParse, "too many arguments"},
		},

		// ═══════════════════════════════════════════════════════════════════════
		// DENIED OPERATIONS
		// ═══════════════════════════════════════════════════════════════════════

		// --- denied/method (collection methods) ---
		{
			name:    "denied/method/bulkWrite",
			input:   `db.coll.bulkWrite([{insertOne: {document: {a: 1}}}]);`,
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: bulkwrite"},
		},
		{
			name:    "denied/method/count",
			input:   "db.coll.count();",
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: count"},
		},
		{
			name:    "denied/method/countWithQuery",
			input:   "db.coll.count({a: {$gt: 1}});",
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: count"},
		},
		{
			name:    "denied/method/countWithLimit",
			input:   "db.coll.count({}, {limit: 3});",
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: count"},
		},
		{
			name:    "denied/method/countWithUnknown",
			input:   "db.coll.count({}, {unknown: 1});",
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: count"},
		},
		{
			name:    "denied/method/createIndex",
			input:   `db.coll.createIndex({category: 1});`,
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: createindex"},
		},
		{
			name:    "denied/method/createIndexes",
			input:   `db.coll.createIndexes([{"category": 1}, {"title": 1}]);`,
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: createindexes"},
		},
		{
			name:    "denied/method/deleteMany",
			input:   `db.coll.deleteMany({status: "A"}, {collation: {locale: "en", numericOrdering: true}});`,
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: deletemany"},
		},
		{
			name:    "denied/method/deleteOne",
			input:   `db.coll.deleteOne({status: "A"});`,
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: deleteone"},
		},
		{
			name:    "denied/method/drop",
			input:   "db.coll.drop();",
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: drop"},
		},
		{
			name:    "denied/method/dropIndex",
			input:   "db.coll.dropIndex('category');",
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: dropindex"},
		},
		{
			name:    "denied/method/dropIndexes",
			input:   "db.coll.dropIndexes(['category', 'tags']);",
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: dropindexes"},
		},
		{
			name:    "denied/method/findAndModify",
			input:   `db.coll.findAndModify({query: {"v": "a"}, update: {$inc: {number: 1}}});`,
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: findandmodify"},
		},
		{
			name:    "denied/method/findOneAndDelete",
			input:   "db.coll.findOneAndDelete({a: 1});",
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: findoneanddelete"},
		},
		{
			name:    "denied/method/findOneAndReplace",
			input:   "db.coll.findOneAndReplace({a: 1}, {a: 2});",
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: findoneandreplace"},
		},
		{
			name:    "denied/method/findOneAndUpdate",
			input:   "db.coll.findOneAndUpdate({a: 1}, {$inc: {a: 5}}, {sort: {a: -1}});",
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: findoneandupdate"},
		},
		{
			name:     "help/collection",
			input:    "db.coll.help()",
			wantHelp: true,
		},
		{
			name:    "denied/method/insertMany",
			input:   "db.coll.insertMany([{a: 1}, {a: 2}]);",
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: insertmany"},
		},
		{
			name:    "denied/method/insertOne",
			input:   `db.coll.insertOne({_id: {"document": "key"}});`,
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: insertone"},
		},
		{
			name:    "denied/method/renameCollection",
			input:   "db.coll.renameCollection('newName');",
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: renamecollection"},
		},
		{
			name:    "denied/method/replaceOne",
			input:   "db.coll.replaceOne({a: 1}, {a: 2});",
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: replaceone"},
		},
		{
			name:    "denied/method/totalIndexSize",
			input:   "db.coll.totalIndexSize();",
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: totalindexsize"},
		},
		{
			name:    "denied/method/updateMany",
			input:   "db.coll.updateMany({a: 1}, {$set: {b: 1}});",
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: updatemany"},
		},
		{
			name:    "denied/method/updateOne",
			input:   "db.coll.updateOne({a: 1}, {$set: {b: 1}});",
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: updateone"},
		},

		// --- denied/db (database methods) ---
		{
			name:    "denied/db/createCollection",
			input:   "db.createCollection('coll')",
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: createcollection"},
		},
		{
			name:    "denied/db/createView",
			input:   "db.createView('myView', 'source', [{$match: {status: 1}}]);",
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: createview"},
		},
		{
			name:    "denied/db/dropDatabase",
			input:   "db.dropDatabase();",
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: dropdatabase"},
		},
		{
			name:    "denied/db/getCollection",
			input:   "db.getCollection('does not exist');",
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: getcollection"},
		},
		{
			name:    "denied/db/getCollectionInfos",
			input:   `db.getCollectionInfos({name: {$regex: "testCollection.*"}});`,
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: getcollectioninfos"},
		},
		{
			name:    "denied/db/getName",
			input:   "db.getName()",
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: getname"},
		},
		{
			name:     "help/database",
			input:    "db.help()",
			wantHelp: true,
		},
		{
			name:    "denied/db/serverStatus",
			input:   "db.serverStatus()",
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: serverstatus"},
		},
		{
			name:    "denied/db/version",
			input:   "db.version()",
			wantErr: &wantError{ErrDeniedCommand, "unsupported method: version"},
		},

		// --- denied/chainer (cursor chainers) ---
		{
			name:    "denied/chainer/collation",
			input:   `db.coll.find({a: "a"}).collation({"locale": "en_US", strength: 1});`,
			wantErr: &wantError{ErrDeniedCommand, "unsupported chainer .collation()"},
		},
		{
			name:    "denied/chainer/comment",
			input:   `db.coll.find().comment("hello")`,
			wantErr: &wantError{ErrDeniedCommand, "unsupported chainer .comment()"},
		},
		{
			name:    "denied/chainer/count",
			input:   `db.coll.find({}, {}, {"comment": "hello"}).count();`,
			wantErr: &wantError{ErrDeniedCommand, "unsupported chainer .count()"},
		},
		{
			name:     "help/cursor",
			input:    "db.coll.find().help()",
			wantHelp: true,
		},
		{
			name:    "denied/chainer/hint",
			input:   "db.coll.find().hint({_id: 1});",
			wantErr: &wantError{ErrDeniedCommand, "unsupported chainer .hint()"},
		},
		{
			name:    "denied/chainer/isClosed",
			input:   "db.coll.find().isClosed();",
			wantErr: &wantError{ErrDeniedCommand, "unsupported chainer .isclosed()"},
		},
		{
			name:    "denied/chainer/isClosedAggregate",
			input:   "db.coll.aggregate([{$sort: {_id: -1}}]).isClosed();",
			wantErr: &wantError{ErrDeniedCommand, "unsupported chainer .isclosed()"},
		},
		{
			name:    "denied/chainer/itcount",
			input:   "db.coll.find().itcount();",
			wantErr: &wantError{ErrDeniedCommand, "unsupported chainer .itcount()"},
		},
		{
			name:    "denied/chainer/max",
			input:   "db.coll.find().max({_id: 2}).hint({_id: 1});",
			wantErr: &wantError{ErrDeniedCommand, "unsupported chainer .max()"},
		},
		{
			name:    "denied/chainer/min",
			input:   "db.coll.find().min({_id: 2}).hint({_id: 1});",
			wantErr: &wantError{ErrDeniedCommand, "unsupported chainer .min()"},
		},
		{
			name:    "denied/chainer/noCursorTimeout",
			input:   "db.coll.find().noCursorTimeout()",
			wantErr: &wantError{ErrDeniedCommand, "unsupported chainer .nocursortimeout()"},
		},
		{
			name:    "denied/chainer/projection",
			input:   "db.coll.find().projection({_id: 0});",
			wantErr: &wantError{ErrDeniedCommand, "unsupported chainer .projection()"},
		},
		{
			name:    "denied/chainer/readConcern",
			input:   "db.coll.find().readConcern('local');",
			wantErr: &wantError{ErrDeniedCommand, "unsupported chainer .readconcern()"},
		},
		{
			name:    "denied/chainer/readPref",
			input:   `db.coll.find({a: "a"}).readPref("secondary");`,
			wantErr: &wantError{ErrDeniedCommand, "unsupported chainer .readpref()"},
		},
		{
			name: "denied/chainer/readPrefAggregate",
			input: `db.coll.aggregate([{$match: {a: "a"}}], {collation: {"locale": "en_US", strength: 1}})
    .readPref("secondary", [{"region": "South", "datacenter": "A"}, {}]);`,
			wantErr: &wantError{ErrDeniedCommand, "unsupported chainer .readpref()"},
		},
		{
			name:    "denied/chainer/returnKey",
			input:   "db.coll.find().returnKey(true);",
			wantErr: &wantError{ErrDeniedCommand, "unsupported chainer .returnkey()"},
		},
		{
			name:    "denied/chainer/tailable",
			input:   "db.coll.find().tailable();",
			wantErr: &wantError{ErrDeniedCommand, "unsupported chainer .tailable()"},
		},
		{
			name:    "denied/chainer/toArray",
			input:   "db.coll.find().toArray();",
			wantErr: &wantError{ErrDeniedCommand, "unsupported chainer .toarray()"},
		},

		// --- denied/runCommand ---
		{
			name:    "denied/runCommand/insert",
			input:   `db.runCommand({"insert": "users", "documents": [{}]})`,
			wantErr: &wantError{ErrDeniedCommand, "command insert is not allowed"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Compile(tt.input)

			if (err != nil) != (tt.wantErr != nil) {
				t.Errorf("Compile() error = %v, wantErr %v", err, tt.wantErr != nil)
				return
			}

			if err != nil {
				compileErr, ok := err.(*CompileError)
				if !ok {
					t.Errorf("Compile() error type = %T, want *CompileError", err)
					return
				}
				if tt.wantErr.Kind != 0 && compileErr.Kind != tt.wantErr.Kind {
					t.Errorf("Compile() error kind = %v, want %v", compileErr.Kind, tt.wantErr.Kind)
				}
				if tt.wantErr.Msg != "" && !strings.Contains(compileErr.Error(), tt.wantErr.Msg) {
					t.Errorf("Compile() error message = %q, want substring %q", compileErr.Error(), tt.wantErr.Msg)
				}
				return
			}

			// Check for help responses
			if tt.wantHelp {
				if got.HelpText == "" {
					t.Error("Compile() expected HelpText to be non-empty")
				}
				return
			}

			if !reflect.DeepEqual(got.Command, tt.wantCmd) {
				t.Errorf("Compile() Command = %v, want %v", got.Command, tt.wantCmd)
			}
			if got.ResultKind != tt.wantKind {
				t.Errorf("Compile() ResultKind = %v, want %v", got.ResultKind, tt.wantKind)
			}
			if got.Namespace != tt.wantNS {
				t.Errorf("Compile() Namespace = %v, want %v", got.Namespace, tt.wantNS)
			}
			if !reflect.DeepEqual(got.Hints, tt.wantHints) {
				t.Errorf("Compile() Hints = %+v, want %+v", got.Hints, tt.wantHints)
			}
		})
	}
}

func TestCompile_AllLiteralTypes(t *testing.T) {
	getFilterValue := func(t *testing.T, input string) any {
		t.Helper()
		got, err := Compile(input)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(got.Command), 2, "Command too short: %v", got.Command)

		var filter bson.D
		for _, elem := range got.Command {
			if elem.Key == "filter" {
				filter = elem.Value.(bson.D)
				break
			}
		}

		require.Len(t, filter, 1)
		require.Equal(t, "v", filter[0].Key)
		return filter[0].Value
	}

	t.Run("ObjectId", func(t *testing.T) {
		v := getFilterValue(t, `db.test.find({"v": ObjectId("7c9d3e4f5a6b78901cdef234")})`)
		oid, ok := v.(bson.ObjectID)
		require.True(t, ok, "expected bson.ObjectID, got %T", v)
		require.Equal(t, "7c9d3e4f5a6b78901cdef234", oid.Hex())
	})

	t.Run("ISODate", func(t *testing.T) {
		v := getFilterValue(t, `db.test.find({"v": ISODate("2024-03-22T14:15:30Z")})`)
		dt, ok := v.(bson.DateTime)
		require.True(t, ok, "expected bson.DateTime, got %T", v)
		tm := dt.Time().UTC()
		require.Equal(t, 2024, tm.Year())
		require.Equal(t, time.March, tm.Month())
		require.Equal(t, 22, tm.Day())
	})

	t.Run("NumberInt", func(t *testing.T) {
		v := getFilterValue(t, `db.test.find({"v": NumberInt(42)})`)
		n, ok := v.(int32)
		require.True(t, ok, "expected int32, got %T", v)
		require.Equal(t, int32(42), n)
	})

	t.Run("NumberLong", func(t *testing.T) {
		v := getFilterValue(t, `db.test.find({"v": NumberLong("8234567890123456789")})`)
		n, ok := v.(int64)
		require.True(t, ok, "expected int64, got %T", v)
		require.Equal(t, int64(8234567890123456789), n)
	})

	t.Run("Double", func(t *testing.T) {
		v := getFilterValue(t, `db.test.find({"v": Double(27.83)})`)
		n, ok := v.(float64)
		require.True(t, ok, "expected float64, got %T", v)
		require.InDelta(t, 27.83, n, 0.0001)
	})

	t.Run("Decimal128", func(t *testing.T) {
		v := getFilterValue(t, `db.test.find({"v": Decimal128("47.53")})`)
		d, ok := v.(bson.Decimal128)
		require.True(t, ok, "expected bson.Decimal128, got %T", v)
		require.Equal(t, "47.53", d.String())
	})

	t.Run("NumberDecimal", func(t *testing.T) {
		v := getFilterValue(t, `db.test.find({"v": NumberDecimal("63.17")})`)
		d, ok := v.(bson.Decimal128)
		require.True(t, ok, "expected bson.Decimal128, got %T", v)
		require.Equal(t, "63.17", d.String())
	})

	t.Run("UUID", func(t *testing.T) {
		v := getFilterValue(t, `db.test.find({"v": UUID("a1b2c3d4-e5f6-7890-ab12-cdef34567890")})`)
		b, ok := v.(bson.Binary)
		require.True(t, ok, "expected bson.Binary, got %T", v)
		require.Equal(t, byte(0x04), b.Subtype, "expected subtype 0x04 (UUID)")
	})

	t.Run("MinKey", func(t *testing.T) {
		v := getFilterValue(t, `db.test.find({"v": MinKey()})`)
		_, ok := v.(bson.MinKey)
		require.True(t, ok, "expected bson.MinKey, got %T", v)
	})

	t.Run("MaxKey", func(t *testing.T) {
		v := getFilterValue(t, `db.test.find({"v": MaxKey()})`)
		_, ok := v.(bson.MaxKey)
		require.True(t, ok, "expected bson.MaxKey, got %T", v)
	})

	t.Run("Timestamp", func(t *testing.T) {
		v := getFilterValue(t, `db.test.find({"v": Timestamp(1, 2)})`)
		ts, ok := v.(bson.Timestamp)
		require.True(t, ok, "expected bson.Timestamp, got %T", v)
		require.Equal(t, uint32(1), ts.T)
		require.Equal(t, uint32(2), ts.I)
	})

	t.Run("array", func(t *testing.T) {
		v := getFilterValue(t, `db.test.find({"v": [1, 2, 3]})`)
		arr, ok := v.(bson.A)
		require.True(t, ok, "expected bson.A, got %T", v)
		require.Len(t, arr, 3)
	})

	t.Run("bool", func(t *testing.T) {
		v := getFilterValue(t, `db.test.find({"v": true})`)
		b, ok := v.(bool)
		require.True(t, ok, "expected bool, got %T", v)
		require.True(t, b)
	})

	t.Run("double", func(t *testing.T) {
		v := getFilterValue(t, `db.test.find({"v": 0.1})`)
		f, ok := v.(float64)
		require.True(t, ok, "expected float64, got %T", v)
		require.InDelta(t, 0.1, f, 0.0001)
	})

	t.Run("emptyArray", func(t *testing.T) {
		v := getFilterValue(t, `db.test.find({"v": []})`)
		arr, ok := v.(bson.A)
		require.True(t, ok, "expected bson.A, got %T", v)
		require.Empty(t, arr)
	})

	t.Run("emptyObject", func(t *testing.T) {
		v := getFilterValue(t, `db.test.find({"v": {}})`)
		obj, ok := v.(bson.D)
		require.True(t, ok, "expected bson.D, got %T", v)
		require.Empty(t, obj)
	})

	t.Run("int", func(t *testing.T) {
		v := getFilterValue(t, `db.test.find({"v": 42})`)
		n, ok := v.(int32)
		require.True(t, ok, "expected int32, got %T", v)
		require.Equal(t, int32(42), n)
	})

	t.Run("null", func(t *testing.T) {
		v := getFilterValue(t, `db.test.find({"v": null})`)
		require.Nil(t, v)
	})

	t.Run("object", func(t *testing.T) {
		v := getFilterValue(t, `db.test.find({"v": {"a": 1}})`)
		obj, ok := v.(bson.D)
		require.True(t, ok, "expected bson.D, got %T", v)
		require.Len(t, obj, 1)
		require.Equal(t, "a", obj[0].Key)
	})

	t.Run("string", func(t *testing.T) {
		v := getFilterValue(t, `db.test.find({"v": "hello"})`)
		s, ok := v.(string)
		require.True(t, ok, "expected string, got %T", v)
		require.Equal(t, "hello", s)
	})

	t.Run("undefined", func(t *testing.T) {
		v := getFilterValue(t, `db.test.find({"v": undefined})`)
		_, ok := v.(bson.Undefined)
		require.True(t, ok, "expected bson.Undefined, got %T", v)
	})

	t.Run("BinData", func(t *testing.T) {
		v := getFilterValue(t, `db.test.find({"v": BinData(0, "eHl6cXdl")})`)
		b, ok := v.(bson.Binary)
		require.True(t, ok, "expected bson.Binary, got %T", v)
		require.Equal(t, byte(0), b.Subtype)
		require.Equal(t, []byte("xyzqwe"), b.Data) // eHl6cXdl is base64 for "xyzqwe"
	})

	t.Run("Date", func(t *testing.T) {
		v := getFilterValue(t, `db.test.find({"v": new Date("2022-08-17T09:25:00Z")})`)
		d, ok := v.(bson.DateTime)
		require.True(t, ok, "expected bson.DateTime, got %T", v)
		require.Equal(t, bson.DateTime(1660728300000), d)
	})
}
