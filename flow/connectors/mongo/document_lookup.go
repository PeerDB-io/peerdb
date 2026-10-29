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
	// keyFields is the documentKey field names in order, joined by keyFieldSeparator. Requests
	// with the same source table and key fields share one query and are matched back by their
	// full key: "_id" alone (unsharded, or sharded on _id) uses an _id $in, and any other key
	// (sharded on another field) uses an $or of full-key equalities.
	keyFields string
	// eventIndices are the indices of the update events in a given chunk that shares the same fullDocument.
	eventIndices []int
	documentKey  []bson.RawElement
	// latestClusterTime is the latest clusterTime among the request's update events.
	latestClusterTime bson.Timestamp
}

const (
	keyFieldSeparator = "\x00"
	idOnlyKeyFields   = "_id"
)

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
		fields := make([]string, len(elements))
		for j, element := range elements {
			fields[j] = element.Key()
		}
		request := &documentLookupRequest{
			sourceTable:       event.sourceTableName,
			documentKey:       elements,
			eventIndices:      []int{i},
			latestClusterTime: event.clusterTime,
			keyFields:         strings.Join(fields, keyFieldSeparator),
		}
		requestsMap[key] = request
		requests = append(requests, request)
	}

	if len(requests) == 0 {
		return 0, nil
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
	fields := strings.Split(requests[0].keyFields, keyFieldSeparator)
	byKey := make(map[string]*documentLookupRequest, len(requests))
	for _, request := range requests {
		byKey[requestKey(request)] = request
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
		request, expected := byKey[documentKeyOf(cursor.Current, fields)]
		if !expected {
			return errors.New("client-side update lookup returned an unexpected document key")
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

// groupLookupRequests splits requests by source table and key fields into queries of up to
// lookupQueryBatchSize requests.
func groupLookupRequests(requests []*documentLookupRequest) [][]*documentLookupRequest {
	type batchKey struct{ sourceTable, keyFields string }
	var groupedRequests [][]*documentLookupRequest
	batches := make(map[batchKey][]*documentLookupRequest)
	for _, request := range requests {
		key := batchKey{request.sourceTable, request.keyFields}
		batches[key] = append(batches[key], request)
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
	switch {
	case requests[0].keyFields == idOnlyKeyFields:
		ids := make(bson.A, 0, len(requests))
		for _, request := range requests {
			ids = append(ids, request.documentKey[0].Value())
		}
		filter = bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}}
	case len(requests) == 1:
		filter = fullKeyFilter(requests[0])
	default:
		branches := make(bson.A, 0, len(requests))
		for _, request := range requests {
			branches = append(branches, fullKeyFilter(request))
		}
		filter = bson.D{{Key: "$or", Value: branches}}
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

// fullKeyFilter matches one document by every documentKey field. Including the shard key keeps
// the query targeted, and matching the full key avoids associating results by _id alone when
// _id is unique only per shard. $eq keeps object values literal instead of interpreting their
// contents as query operators.
func fullKeyFilter(request *documentLookupRequest) bson.D {
	filter := make(bson.D, 0, len(request.documentKey))
	for _, element := range request.documentKey {
		filter = append(filter, bson.E{Key: element.Key(), Value: bson.D{{Key: "$eq", Value: element.Value()}}})
	}
	return filter
}

func requestKey(request *documentLookupRequest) string {
	var key strings.Builder
	for _, element := range request.documentKey {
		key.WriteString(rawValueKey(element.Value()))
	}
	return key.String()
}

// documentKeyOf rebuilds a looked-up document's key in the same form as requestKey. A shard key
// field can be a dotted path, and a missing shard key field reads as null, as in documentKey.
func documentKeyOf(document bson.Raw, fields []string) string {
	var key strings.Builder
	for _, field := range fields {
		value, err := document.LookupErr(strings.Split(field, ".")...)
		if err != nil {
			value = bson.RawValue{Type: bson.TypeNull}
		}
		key.WriteString(rawValueKey(value))
	}
	return key.String()
}

func lookupCollectionMissing(err error) bool {
	var commandError mongo.CommandError
	return errors.As(err, &commandError) && commandError.HasErrorCode(26) // NamespaceNotFound
}

// rawValueKey identifies a BSON value by its exact encoding. documentKey values are copied from the
// stored document, so a looked-up document's key fields have the same encoding. A document whose
// shard key changed after the update does not match the old key's query at all.
func rawValueKey(value bson.RawValue) string {
	return string([]byte{byte(value.Type)}) + string(value.Value)
}

func maxTimestamp(a, b bson.Timestamp) bson.Timestamp {
	if a.T > b.T || (a.T == b.T && a.I > b.I) {
		return a
	}
	return b
}
