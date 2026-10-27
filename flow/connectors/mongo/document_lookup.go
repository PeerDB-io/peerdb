package connmongo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"golang.org/x/sync/errgroup"
)

const (
	// TODO: make these configurable
	lookupQueryBatchSize   = 64
	lookupQueryParallelism = 4
)

type documentLookupKey struct {
	sourceTable string
	key         string
}

type documentLookupRequest struct {
	// fully qualified names (e.g. database.collection)
	sourceTable string
	// eventIndices are the indices of the update events in a given chunk that shares the same fullDocument.
	eventIndices []int
	documentKey  []bson.RawElement
	// latestClusterTime is the latest clusterTime among the request's update events.
	latestClusterTime bson.Timestamp
	// a document lookup is batchable if _id is the whole document key
	// (the collection is unsharded or sharded on _id), so the request
	// can share an _id $in query and be matched back by _id. Otherwise,
	// the request is deemed not batchable and executes its own full-key query.
	batchable bool
}

// hydrateUpdateEvents hydrates the update events' `fullDocument` field using client-side lookup.
func (c *MongoConnector) hydrateUpdateEvents(ctx context.Context, events []encodedMongoEvent) (int64, error) {
	var requests []*documentLookupRequest
	requestsMap := make(map[documentLookupKey]*documentLookupRequest)
	for i := range events {
		event := &events[i]
		if event.operationType != operationTypeUpdate || event.maybeFullDocument != nil {
			continue
		}
		if event.clusterTime == (bson.Timestamp{}) {
			return 0, errors.New("client-side update lookup requires a clusterTime")
		}

		key := documentLookupKey{sourceTable: event.sourceTableName, key: string(event.documentKey)}
		if request := requestsMap[key]; request != nil {
			request.latestClusterTime = maxTimestamp(request.latestClusterTime, event.clusterTime)
			request.eventIndices = append(request.eventIndices, i)
			continue
		}

		elements, err := event.documentKey.Elements()
		if err != nil || len(elements) == 0 || event.documentKey.Lookup("_id").IsZero() {
			// should never happen
			return 0, errors.New("client-side update lookup requires a documentKey containing _id")
		}
		request := &documentLookupRequest{
			sourceTable:       event.sourceTableName,
			documentKey:       elements,
			eventIndices:      []int{i},
			latestClusterTime: event.clusterTime,
		}
		request.batchable = len(elements) == 1
		requestsMap[key] = request
		requests = append(requests, request)
	}

	groupedRequests := groupLookupRequests(requests)

	pref := protoToReadPref[c.config.ReadPreference]
	var fetchedSize atomic.Int64
	group, queryCtx := errgroup.WithContext(ctx)
	group.SetLimit(lookupQueryParallelism)
	for _, reqs := range groupedRequests {
		group.Go(func() error {
			return c.batchLookup(queryCtx, pref, events, reqs, &fetchedSize)
		})
	}
	err := group.Wait()
	return fetchedSize.Load(), err
}

// batchLookup runs one lookup query and sets each matched document on its request's events.
// fullDocument may be missing when an update is followed by a delete, just as with native
// updateLookup. If any query fails, the chunk is never decoded, so events hydrated before
// the failure are discarded with it.
func (c *MongoConnector) batchLookup(
	ctx context.Context, pref *readpref.ReadPref, events []encodedMongoEvent, requests []*documentLookupRequest, fetchedSize *atomic.Int64,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var byID map[string]*documentLookupRequest
	if requests[0].batchable {
		byID = make(map[string]*documentLookupRequest, len(requests))
		for _, request := range requests {
			byID[rawValueKey(request.documentKey[0].Value())] = request
		}
	}
	db, collection, _ := strings.Cut(requests[0].sourceTable, ".")
	command := buildBatchLookupCommand(collection, requests)
	queryCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	cursor, err := c.client.Database(db).RunCommandCursor(queryCtx, command, options.RunCmd().SetReadPreference(pref))
	if err != nil {
		if lookupCollectionMissing(err) {
			c.logger.Warn("client-side update lookup found no collection, skipping",
				slog.String("collection", requests[0].sourceTable))
			return nil
		}
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = cursor.Close(closeCtx)
	}()
	for cursor.Next(queryCtx) {
		request := requests[0]
		if byID != nil {
			var expected bool
			request, expected = byID[rawValueKey(cursor.Current.Lookup("_id"))]
			if !expected {
				return errors.New("client-side update lookup returned an unexpected document key")
			}
		}
		if events[request.eventIndices[0]].maybeFullDocument != nil {
			return fmt.Errorf("client-side update lookup returned multiple documents for one document key in %s",
				request.sourceTable)
		}
		fetchedSize.Add(int64(len(cursor.Current)))
		document := slices.Clone(cursor.Current)
		for _, i := range request.eventIndices {
			events[i].maybeFullDocument = &document
		}
	}
	if err := cursor.Err(); err != nil {
		if lookupCollectionMissing(err) {
			c.logger.Warn("client-side update lookup found no collection, skipping",
				slog.String("collection", requests[0].sourceTable))
			return nil
		}
		return err
	}
	return nil
}

// groupLookupRequests splits requests by source table for query batching. Batchable requests
// share $in queries of up to lookupQueryBatchSize per collection, and remaining requests
// get their own queries unbatched.
func groupLookupRequests(requests []*documentLookupRequest) [][]*documentLookupRequest {
	var groupedRequests [][]*documentLookupRequest
	batches := make(map[string][]*documentLookupRequest)
	for _, request := range requests {
		if request.batchable {
			batches[request.sourceTable] = append(batches[request.sourceTable], request)
		} else {
			groupedRequests = append(groupedRequests, []*documentLookupRequest{request})
		}
	}
	for _, batch := range batches {
		groupedRequests = slices.AppendSeq(groupedRequests, slices.Chunk(batch, lookupQueryBatchSize))
	}
	return groupedRequests
}

func buildBatchLookupCommand(collection string, requests []*documentLookupRequest) bson.D {
	var floor bson.Timestamp
	for _, request := range requests {
		floor = maxTimestamp(floor, request.latestClusterTime)
	}
	var filter bson.D
	if requests[0].batchable {
		ids := make(bson.A, 0, len(requests))
		for _, request := range requests {
			ids = append(ids, request.documentKey[0].Value())
		}
		filter = bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}}
	} else {
		// Full-key equality retains shard targeting and avoids associating results by _id alone
		// when it is not globally unique. $eq keeps object-valued IDs literal instead of
		// interpreting their contents as query operators.
		for _, element := range requests[0].documentKey {
			filter = append(filter, bson.E{Key: element.Key(), Value: bson.D{{Key: "$eq", Value: element.Value()}}})
		}
	}
	return bson.D{
		{Key: "aggregate", Value: collection},
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
}

func lookupCollectionMissing(err error) bool {
	var commandError mongo.CommandError
	return errors.As(err, &commandError) && commandError.HasErrorCode(26) // NamespaceNotFound
}

// rawValueKey identifies a BSON value by its exact encoding. _id never changes, so a looked-up
// document's _id has the same encoding as the _id in its event's documentKey.
func rawValueKey(value bson.RawValue) string {
	return string([]byte{byte(value.Type)}) + string(value.Value)
}

func maxTimestamp(a, b bson.Timestamp) bson.Timestamp {
	if a.T > b.T || (a.T == b.T && a.I > b.I) {
		return a
	}
	return b
}
