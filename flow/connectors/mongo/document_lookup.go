package connmongo

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"golang.org/x/sync/errgroup"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
)

const (
	documentLookupWindowSize  = 256
	documentLookupQuerySize   = 64
	documentLookupConcurrency = 4
	documentLookupMaxBytes    = 64 << 20
)

func supportsParallelDocumentLookup(version string, variant protos.DatabaseVariant) bool {
	majorText, _, _ := strings.Cut(version, ".")
	major, err := strconv.Atoi(majorText)
	return err == nil && major >= 6 && variant != protos.DatabaseVariant_AWS_DOCUMENTDB
}

// Native lookup targets sharded keys with simple collation, then matches using
// the collection default. An external aggregate cannot express that split, so
// retain native updateLookup for collections with a non-simple collation.
func (c *MongoConnector) parallelLookupCollectionsSupported(
	ctx context.Context, mappings map[string]model.SourceTableMapping,
) (map[string]bson.Binary, error) {
	databases := make(map[string][]string)
	collectionUUIDs := make(map[string]bson.Binary, len(mappings))
	for namespace := range mappings {
		database, collection, ok := strings.Cut(namespace, ".")
		if !ok || database == "" || collection == "" {
			return nil, fmt.Errorf("invalid MongoDB namespace %q", namespace)
		}
		databases[database] = append(databases[database], collection)
	}
	for database, names := range databases {
		specs, err := c.client.Database(database).ListCollectionSpecifications(ctx,
			bson.D{{Key: "name", Value: bson.D{{Key: "$in", Value: names}}}})
		if err != nil {
			return nil, err
		}
		if len(specs) != len(names) {
			return nil, nil
		}
		for i := range specs {
			uuid := specs[i].UUID
			if uuid == nil || uuid.Subtype != 4 || len(uuid.Data) != 16 {
				return nil, nil
			}
			collation := specs[i].Options.Lookup("collation")
			if !collation.IsZero() {
				document, ok := collation.DocumentOK()
				if !ok {
					return nil, nil
				}
				locale, ok := document.Lookup("locale").StringValueOK()
				if !ok || locale != "simple" {
					return nil, nil
				}
			}
			collectionUUIDs[database+"."+specs[i].Name] = bson.Binary{Subtype: 4, Data: slices.Clone(uuid.Data)}
		}
	}
	return collectionUUIDs, nil
}

var errDocumentLookupWindowBytes = errors.New("MongoDB document lookup exceeded the 64 MiB window limit")

type pendingDocumentLookup struct {
	resumeToken string
	event       encodedMongoEvent
}

type documentLookupKey struct {
	namespace string
	key       string
}

type documentLookupRequest struct {
	key       documentLookupKey
	filter    bson.D
	readTime  bson.Timestamp
	id        bson.ObjectID
	batchable bool
}

type documentLookupQuery struct {
	documents map[documentLookupKey]bson.Raw
	requests  []*documentLookupRequest
}

// lookupUpdates hydrates update slots in a bounded source-ordered window. Other
// event types retain their own images and positions. Like native updateLookup,
// queries wait for the source event time, not the operationTime of an earlier
// lookup (which can move all the way to the present while replaying a backlog).
func (c *MongoConnector) lookupUpdates(
	ctx context.Context, pending []pendingDocumentLookup, collectionUUIDs map[string]bson.Binary,
) (int64, error) {
	if len(pending) > documentLookupWindowSize {
		return 0, errors.New("MongoDB document lookup window is too large")
	}
	keys := make(map[int]documentLookupKey)
	requests := make(map[documentLookupKey]*documentLookupRequest)
	var ordered []*documentLookupRequest
	for i := range pending {
		event := &pending[i].event
		if event.operationType != operationTypeUpdate || event.maybeFullDocument != nil {
			continue
		}
		if event.clusterTime == (bson.Timestamp{}) {
			return 0, errors.New("MongoDB document lookup requires a clusterTime")
		}
		elements, err := event.documentKey.Elements()
		if err != nil || len(elements) == 0 || event.documentKey.Lookup("_id").IsZero() {
			return 0, errors.New("MongoDB document lookup requires a documentKey containing _id")
		}
		key := documentLookupKey{namespace: event.sourceTableName, key: string(event.documentKey)}
		keys[i] = key
		if request := requests[key]; request != nil {
			request.readTime = laterDocumentLookupTime(request.readTime, event.clusterTime)
			continue
		}
		request := &documentLookupRequest{key: key, readTime: event.clusterTime}
		request.id, request.batchable = event.documentKey.Lookup("_id").ObjectIDOK()
		request.batchable = request.batchable && len(elements) == 1
		if !request.batchable {
			// Full-key equality retains shard targeting and avoids associating results
			// by _id alone when it is not globally unique. $eq keeps object-valued
			// IDs literal instead of interpreting their contents as query operators.
			var expressions bson.A
			for _, element := range elements {
				if strings.HasPrefix(element.Key(), "$") {
					expressions = append(expressions, bson.D{{Key: "$eq", Value: bson.A{
						bson.D{{Key: "$getField", Value: bson.D{
							{Key: "input", Value: "$$ROOT"},
							{Key: "field", Value: bson.D{{Key: "$literal", Value: element.Key()}}},
						}}},
						bson.D{{Key: "$literal", Value: element.Value()}},
					}}})
				} else {
					request.filter = append(request.filter, bson.E{Key: element.Key(), Value: bson.D{{Key: "$eq", Value: element.Value()}}})
				}
			}
			if len(expressions) > 0 {
				request.filter = append(request.filter, bson.E{Key: "$expr", Value: bson.D{{Key: "$and", Value: expressions}}})
			}
		}
		requests[key] = request
		ordered = append(ordered, request)
	}
	var queries []documentLookupQuery
	collections := make(map[string][]*documentLookupRequest)
	for _, request := range ordered {
		if request.batchable {
			collections[request.key.namespace] = append(collections[request.key.namespace], request)
		} else {
			queries = append(queries, documentLookupQuery{requests: []*documentLookupRequest{request}})
		}
	}
	for _, collection := range collections {
		for chunk := range slices.Chunk(collection, documentLookupQuerySize) {
			queries = append(queries, documentLookupQuery{requests: chunk})
		}
	}
	var fetched atomic.Int64
	group, queryCtx := errgroup.WithContext(ctx)
	group.SetLimit(documentLookupConcurrency)
	for i := range queries {
		query := &queries[i]
		group.Go(func() error {
			var err error
			query.documents, err = c.lookupUpdateDocuments(queryCtx, query.requests, &fetched, collectionUUIDs)
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return fetched.Load(), err
	}
	if err := ctx.Err(); err != nil {
		return fetched.Load(), err
	}
	images := make(map[documentLookupKey]bson.Raw, len(requests))
	for i := range queries {
		for key, raw := range queries[i].documents {
			images[key] = raw
		}
	}
	// Only mutate the window after every query succeeds. A missing document is
	// normal when an update is followed by a delete, just as with updateLookup.
	for i, key := range keys {
		if raw := images[key]; raw != nil {
			pending[i].event.maybeFullDocument = &raw
		}
	}
	return fetched.Load(), nil
}

func (c *MongoConnector) lookupUpdateDocuments(
	ctx context.Context, requests []*documentLookupRequest, fetched *atomic.Int64,
	collectionUUIDs map[string]bson.Binary,
) (map[documentLookupKey]bson.Raw, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	namespace := requests[0].key.namespace
	database, name, ok := strings.Cut(namespace, ".")
	if !ok || database == "" || name == "" {
		return nil, errors.New("invalid MongoDB document lookup namespace")
	}
	collectionUUID, ok := collectionUUIDs[namespace]
	if !ok {
		return nil, errors.New("MongoDB document lookup requires verified collection metadata")
	}
	pref, ok := protoToReadPref[c.config.ReadPreference]
	if !ok {
		return nil, errors.New("invalid MongoDB document lookup read preference")
	}
	var floor bson.Timestamp
	byID := make(map[bson.ObjectID]documentLookupKey, len(requests))
	ids := make([]bson.ObjectID, 0, len(requests))
	for _, request := range requests {
		floor = laterDocumentLookupTime(floor, request.readTime)
		ids = append(ids, request.id)
		byID[request.id] = request.key
	}
	filter := requests[0].filter
	if requests[0].batchable {
		filter = bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}}
	}
	command := bson.D{
		{Key: "aggregate", Value: name},
		// Guard the current metadata, not the historical event UUID. If the collection
		// is replaced after preflight, retry so its collation is checked again.
		{Key: "collectionUUID", Value: collectionUUID},
		{Key: "pipeline", Value: mongo.Pipeline{
			bson.D{{Key: "$match", Value: filter}},
			bson.D{{Key: "$limit", Value: len(requests) + 1}},
		}},
		// One extra slot lets the server report exhaustion without an empty getMore,
		// while still detecting duplicate matches instead of silently picking one.
		{Key: "cursor", Value: bson.D{{Key: "batchSize", Value: len(requests) + 1}}},
		{Key: "readConcern", Value: bson.D{{Key: "level", Value: "majority"}, {Key: "afterClusterTime", Value: floor}}},
		{Key: "comment", Value: "PeerDB parallel document lookup"},
	}
	queryCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	cursor, err := c.client.Database(database).RunCommandCursor(queryCtx, command, options.RunCmd().SetReadPreference(pref))
	if err != nil {
		if lookupCollectionMissing(err) {
			return nil, nil
		}
		return nil, err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = cursor.Close(closeCtx)
	}()
	documents := make(map[documentLookupKey]bson.Raw, len(requests))
	for cursor.Next(queryCtx) {
		key := requests[0].key
		if requests[0].batchable {
			id, valid := cursor.Current.Lookup("_id").ObjectIDOK()
			var expected bool
			key, expected = byID[id]
			if !valid || !expected {
				return nil, errors.New("MongoDB document lookup returned an unexpected key")
			}
		}
		if _, exists := documents[key]; exists {
			return nil, fmt.Errorf("MongoDB document lookup returned multiple matches for a document key in %s", namespace)
		}
		// This bounds retained lookup BSON, not driver buffers or decoded records.
		// The caller subdivides oversized windows and retries them in source order.
		if fetched.Add(int64(len(cursor.Current))) > documentLookupMaxBytes {
			return nil, errDocumentLookupWindowBytes
		}
		documents[key] = slices.Clone(cursor.Current)
	}
	if err := cursor.Err(); err != nil {
		if lookupCollectionMissing(err) {
			return nil, nil
		}
		return nil, err
	}
	return documents, nil
}

func lookupCollectionMissing(err error) bool {
	var commandError mongo.CommandError
	return errors.As(err, &commandError) &&
		commandError.HasErrorCode(26) // NamespaceNotFound.
}

func laterDocumentLookupTime(a, b bson.Timestamp) bson.Timestamp {
	if a.T > b.T || (a.T == b.T && a.I > b.I) {
		return a
	}
	return b
}
