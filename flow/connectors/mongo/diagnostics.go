package connmongo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

const (
	topologyChangesWindow        = 24 * time.Hour
	changeStreamOpsSamples       = 3
	changeStreamOpsInterval      = 10 * time.Second
	changeStreamCommentRegex     = "^PeerDB changeStream for mirror "
	topologyChangesEventsPattern = "movePrimary|moveCollection|reshard|moveChunk|moveRange|unshardCollection"
)

type diagnosticCheck struct {
	run         func(context.Context) ([]bson.Raw, error)
	name        string
	description string
	query       string
	samples     int
	interval    time.Duration
}

func (c *MongoConnector) SourceDiagnostics(ctx context.Context) ([]*protos.SourceDiagnosticsCheck, error) {
	changeStreamOps, err := c.changeStreamOpsCheck()
	if err != nil {
		return nil, err
	}
	checks := []diagnosticCheck{
		c.listShardsCheck(),
		c.topologyChangesCheck(time.Now().UTC().Add(-topologyChangesWindow)),
		changeStreamOps,
	}

	report := make([]*protos.SourceDiagnosticsCheck, 0, len(checks))
	for _, check := range checks {
		result, err := runDiagnosticCheck(ctx, check)
		if err != nil {
			return nil, err
		}
		report = append(report, &protos.SourceDiagnosticsCheck{
			Name:        check.name,
			Description: check.description,
			Query:       check.query,
			Result:      result,
		})
	}
	return report, nil
}

func runDiagnosticCheck(ctx context.Context, check diagnosticCheck) ([]*protos.SourceDiagnosticsRun, error) {
	runs := make([]*protos.SourceDiagnosticsRun, 0, check.samples)
	for i := range check.samples {
		if i > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(check.interval):
			}
		}

		run := &protos.SourceDiagnosticsRun{RanAt: time.Now().UTC().Format(time.RFC3339Nano)}
		docs, err := check.run(ctx)
		if err == nil {
			run.Response, err = docsToExtJSON(docs)
		}
		if err != nil {
			run.Error = err.Error()
		}
		runs = append(runs, run)
	}
	return runs, nil
}

func docsToExtJSON(docs []bson.Raw) ([]string, error) {
	rows := make([]string, 0, len(docs))
	for _, doc := range docs {
		row, err := bson.MarshalExtJSON(doc, false, false)
		if err != nil {
			return nil, fmt.Errorf("failed to convert document to extended JSON: %w", err)
		}
		rows = append(rows, string(row))
	}
	return rows, nil
}

func collectCursor(ctx context.Context, cursor *mongo.Cursor, err error) ([]bson.Raw, error) {
	if err != nil {
		return nil, err
	}
	var docs []bson.Raw
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, err
	}
	return docs, nil
}

func (c *MongoConnector) listShardsCheck() diagnosticCheck {
	return diagnosticCheck{
		name:        "List shards",
		description: "Lists all the shards in the MongoDB cluster and their status.",
		query:       `db.getSiblingDB("config").getCollection("shards").find({})`,
		samples:     1,
		run: func(ctx context.Context) ([]bson.Raw, error) {
			cursor, err := c.client.Database("config").Collection("shards").Find(ctx, bson.D{})
			return collectCursor(ctx, cursor, err)
		},
	}
}

func (c *MongoConnector) topologyChangesCheck(since time.Time) diagnosticCheck {
	sinceStr := since.Format(time.RFC3339)
	filter := bson.D{
		{Key: "time", Value: bson.D{{Key: "$gte", Value: since}}},
		{Key: "what", Value: bson.Regex{Pattern: topologyChangesEventsPattern}},
	}
	projection := bson.D{
		{Key: "time", Value: 1},
		{Key: "what", Value: 1},
		{Key: "ns", Value: 1},
		{Key: "details.from", Value: 1},
		{Key: "details.to", Value: 1},
		{Key: "details.donorShards", Value: 1},
		{Key: "details.recipientShards", Value: 1},
	}
	return diagnosticCheck{
		name:        "Sharding topology changes",
		description: fmt.Sprintf("Data movement across shards from %s to now.", sinceStr),
		query: fmt.Sprintf(`const since = ISODate(%q);

db.getSiblingDB("config").changelog.find(
  { time: { $gte: since },
    what: /%s/ },
  { time: 1, what: 1, ns: 1, "details.from": 1, "details.to": 1,
    "details.donorShards": 1, "details.recipientShards": 1 }
).sort({ time: 1 })`, sinceStr, topologyChangesEventsPattern),
		samples: 1,
		run: func(ctx context.Context) ([]bson.Raw, error) {
			cursor, err := c.client.Database("config").Collection("changelog").Find(ctx, filter,
				options.Find().SetProjection(projection).SetSort(bson.D{{Key: "time", Value: 1}}))
			return collectCursor(ctx, cursor, err)
		},
	}
}

func (c *MongoConnector) changeStreamOpsCheck() (diagnosticCheck, error) {
	currentOp := bson.D{
		{Key: "allUsers", Value: true},
		{Key: "idleCursors", Value: true},
		{Key: "localOps", Value: false},
	}

	commentMatch := bson.D{{Key: "$regex", Value: changeStreamCommentRegex}}
	matchFields := []string{"command.comment", "originatingCommand.comment", "cursor.originatingCommand.comment"}
	or := make(bson.A, 0, len(matchFields))
	for _, field := range matchFields {
		or = append(or, bson.D{{Key: field, Value: commentMatch}})
	}

	projectFields := []string{
		"shard", "host", "appName", "opid", "op", "active", "secs_running",
		"waitingForLock", "command", "originatingCommand", "cursor",
	}
	project := make(bson.D, 0, len(projectFields))
	for _, field := range projectFields {
		project = append(project, bson.E{Key: field, Value: 1})
	}

	pipeline := mongo.Pipeline{
		{{Key: "$currentOp", Value: currentOp}},
		{{Key: "$match", Value: bson.D{{Key: "$or", Value: or}}}},
		{{Key: "$project", Value: project}},
	}
	query, err := aggregateQuery("admin", pipeline)
	if err != nil {
		return diagnosticCheck{}, err
	}
	return diagnosticCheck{
		name:        "PeerDB change-stream operations",
		description: "Checks the status and configuration of PeerDB change-stream operations on the MongoDB source.",
		query:       query,
		samples:     changeStreamOpsSamples,
		interval:    changeStreamOpsInterval,
		run: func(ctx context.Context) ([]bson.Raw, error) {
			cursor, err := c.client.Database("admin").Aggregate(ctx, pipeline)
			return collectCursor(ctx, cursor, err)
		},
	}, nil
}

// aggregateQuery renders the mongosh equivalent of running pipeline on database.
// It is only valid for pipelines whose values are plain JSON types:
// relaxed extended JSON renders dates and regexes as $date/$regularExpression documents.
func aggregateQuery(database string, pipeline mongo.Pipeline) (string, error) {
	// extended JSON marshaling needs a document at the top level
	doc, err := bson.MarshalExtJSON(bson.D{{Key: "pipeline", Value: pipeline}}, false, false)
	if err != nil {
		return "", fmt.Errorf("failed to convert pipeline to extended JSON: %w", err)
	}
	var wrapper struct {
		Pipeline json.RawMessage `json:"pipeline"`
	}
	if err := json.Unmarshal(doc, &wrapper); err != nil {
		return "", fmt.Errorf("failed to extract pipeline from extended JSON: %w", err)
	}
	var indented bytes.Buffer
	if err := json.Indent(&indented, wrapper.Pipeline, "", "  "); err != nil {
		return "", fmt.Errorf("failed to indent pipeline: %w", err)
	}
	return fmt.Sprintf("db.getSiblingDB(%q).aggregate(%s)", database, indented.String()), nil
}
