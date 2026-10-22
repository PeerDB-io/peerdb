package connmongo

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"golang.org/x/sync/errgroup"
)

const (
	documentLookupWindowSize  = 64
	documentLookupQuerySize   = 16
	documentLookupConcurrency = 4
	documentLookupMaxBytes    = 64 << 20
)

type pendingDocumentLookup struct {
	resumeToken string
	event       encodedMongoEvent
}

type documentLookupCollection struct {
	namespace string
	uuid      [16]byte
}

type documentLookupKey struct {
	collection documentLookupCollection
	id         bson.ObjectID
}

type documentLookupQuery struct {
	documents  map[bson.ObjectID]bson.Raw
	ids        []bson.ObjectID
	collection documentLookupCollection
	readTime   bson.Timestamp
}

// lookupUpdates hydrates one update-only window. Repeated keys share one image;
// the next window reads after this window's successful read time. The caller
// emits events in source order and persists that time with the resume token.
func (c *MongoConnector) lookupUpdates(
	ctx context.Context, pending []pendingDocumentLookup,
) (bson.Timestamp, int64, error) {
	floor := c.documentLookupWatermark
	if len(pending) == 0 {
		return floor, 0, nil
	}
	if len(pending) > documentLookupWindowSize {
		return floor, 0, errors.New("MongoDB document lookup window exceeds 64 events")
	}
	keys := make([]documentLookupKey, len(pending))
	seen := make(map[documentLookupKey]bool, len(pending))
	collections := make(map[documentLookupCollection][]bson.ObjectID)
	for i := range pending {
		event := &pending[i].event
		if event.operationType != operationTypeUpdate || event.maybeFullDocument != nil || event.clusterTime == (bson.Timestamp{}) {
			return floor, 0, errors.New("MongoDB document lookup requires an update without fullDocument and with clusterTime")
		}
		if event.collectionUUID.Subtype != 4 || len(event.collectionUUID.Data) != 16 {
			return floor, 0, errors.New("MongoDB document lookup requires a collection UUID")
		}
		elements, err := event.documentKey.Elements()
		if err != nil || len(elements) != 1 || elements[0].Key() != "_id" {
			return floor, 0, errors.New("MongoDB parallel document lookup only supports documentKey containing exactly _id")
		}
		id, ok := elements[0].Value().ObjectIDOK()
		if !ok {
			return floor, 0, errors.New("MongoDB parallel document lookup only supports ObjectID keys")
		}
		collection := documentLookupCollection{namespace: event.sourceTableName, uuid: [16]byte(event.collectionUUID.Data)}
		keys[i] = documentLookupKey{collection: collection, id: id}
		if !seen[keys[i]] {
			seen[keys[i]] = true
			collections[collection] = append(collections[collection], id)
		}
		floor = laterDocumentLookupTime(floor, event.clusterTime)
	}
	var queries []documentLookupQuery
	for collection, ids := range collections {
		for chunk := range slices.Chunk(ids, documentLookupQuerySize) {
			queries = append(queries, documentLookupQuery{collection: collection, ids: chunk})
		}
	}
	var fetched atomic.Int64
	group, queryCtx := errgroup.WithContext(ctx)
	group.SetLimit(documentLookupConcurrency)
	for i := range queries {
		query := &queries[i]
		group.Go(func() error {
			if err := queryCtx.Err(); err != nil {
				return err
			}
			var err error
			query.documents, query.readTime, err = c.lookupUpdateDocuments(queryCtx, query.collection, query.ids, floor, &fetched)
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return floor, fetched.Load(), err
	}
	if err := ctx.Err(); err != nil {
		return floor, fetched.Load(), err
	}
	images := make(map[documentLookupKey]bson.Raw, len(seen))
	for i := range queries {
		query := &queries[i]
		floor = laterDocumentLookupTime(floor, query.readTime)
		for id, raw := range query.documents {
			images[documentLookupKey{collection: query.collection, id: id}] = raw
		}
	}
	// No partial hydration or emission if any query failed or a document vanished.
	for i := range pending {
		raw := images[keys[i]]
		pending[i].event.maybeFullDocument = &raw
	}
	return floor, fetched.Load(), nil
}

func (c *MongoConnector) lookupUpdateDocuments(
	ctx context.Context, collection documentLookupCollection, ids []bson.ObjectID,
	floor bson.Timestamp, fetched *atomic.Int64,
) (map[bson.ObjectID]bson.Raw, bson.Timestamp, error) {
	database, name, ok := strings.Cut(collection.namespace, ".")
	if !ok || database == "" || name == "" {
		return nil, bson.Timestamp{}, errors.New("invalid MongoDB document lookup namespace")
	}
	pref, ok := protoToReadPref[c.config.ReadPreference]
	if !ok {
		return nil, bson.Timestamp{}, errors.New("invalid MongoDB document lookup read preference")
	}
	command := bson.D{
		{Key: "aggregate", Value: name},
		{Key: "collectionUUID", Value: bson.Binary{Subtype: 4, Data: collection.uuid[:]}},
		{Key: "pipeline", Value: mongo.Pipeline{
			bson.D{{Key: "$match", Value: bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}}}},
			bson.D{{Key: "$limit", Value: len(ids) + 1}},
		}},
		{Key: "cursor", Value: bson.D{{Key: "batchSize", Value: documentLookupQuerySize}}},
		{Key: "collation", Value: bson.D{{Key: "locale", Value: "simple"}}},
		{Key: "readConcern", Value: bson.D{{Key: "level", Value: "majority"}, {Key: "afterClusterTime", Value: floor}}},
		{Key: "comment", Value: "PeerDB parallel document lookup"},
	}
	// Each concurrent query owns its session. Explicit readConcern is required on
	// raw commands; response operationTime carries the fence across windows/restarts.
	session, err := c.client.StartSession(options.Session().SetCausalConsistency(false))
	if err != nil {
		return nil, bson.Timestamp{}, err
	}
	defer session.EndSession(context.WithoutCancel(ctx))
	queryCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	sessionCtx := mongo.NewSessionContext(queryCtx, session)
	cursor, err := c.client.Database(database).RunCommandCursor(sessionCtx, command, options.RunCmd().SetReadPreference(pref))
	if err != nil {
		return nil, bson.Timestamp{}, err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = cursor.Close(mongo.NewSessionContext(closeCtx, session))
	}()
	documents := make(map[bson.ObjectID]bson.Raw, len(ids))
	for cursor.Next(sessionCtx) {
		id, valid := cursor.Current.Lookup("_id").ObjectIDOK()
		if !valid || !slices.Contains(ids, id) {
			return nil, bson.Timestamp{}, errors.New("MongoDB document lookup returned an unexpected key")
		}
		if _, exists := documents[id]; exists {
			return nil, bson.Timestamp{}, errors.New("MongoDB document lookup returned duplicate _id values")
		}
		// One shared reservation counter bounds retained BSON across every query.
		// Driver response buffers and Go object overhead are outside this bound.
		if fetched.Add(int64(len(cursor.Current))) > documentLookupMaxBytes {
			return nil, bson.Timestamp{}, errors.New("MongoDB document lookup exceeded the 64 MiB window limit")
		}
		documents[id] = slices.Clone(cursor.Current)
	}
	if err := cursor.Err(); err != nil {
		return nil, bson.Timestamp{}, err
	}
	if len(documents) != len(ids) {
		return nil, bson.Timestamp{}, fmt.Errorf("MongoDB document lookup found %d of %d documents in %s; reconciliation required",
			len(documents), len(ids), collection.namespace)
	}
	readTime := session.OperationTime()
	if readTime == nil || laterDocumentLookupTime(*readTime, floor) != *readTime {
		return nil, bson.Timestamp{}, errors.New("MongoDB document lookup response is missing a valid operationTime")
	}
	return documents, *readTime, nil
}

func laterDocumentLookupTime(a, b bson.Timestamp) bson.Timestamp {
	if a.T > b.T || (a.T == b.T && a.I > b.I) {
		return a
	}
	return b
}
