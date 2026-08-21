package connmongo

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/PeerDB-io/peerdb/flow/alerting"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type operationType string

const (
	operationTypeInsert  operationType = "insert"
	operationTypeUpdate  operationType = "update"
	operationTypeReplace operationType = "replace"
	operationTypeDelete  operationType = "delete"
	operationTypeDrop    operationType = "drop"
	operationTypeRename  operationType = "rename"
)

func parseOperationType(s string) (operationType, bool) {
	switch op := operationType(s); op {
	case operationTypeInsert, operationTypeUpdate, operationTypeReplace, operationTypeDelete:
		return op, true
	default:
		return "", false
	}
}

type Namespace struct {
	Db   string `bson:"db"`
	Coll string `bson:"coll"`
}

type ChangeEvent struct {
	FullDocument  *bson.Raw      `bson:"fullDocument,omitempty"`
	WallTime      *time.Time     `bson:"wallTime,omitempty"`
	Ns            Namespace      `bson:"ns"`
	OperationType string         `bson:"operationType"`
	DocumentKey   bson.Raw       `bson:"documentKey,omitempty"`
	ClusterTime   bson.Timestamp `bson:"clusterTime"`
}

const mongoClockOffsetTTL = time.Hour

// getMongoClockOffset returns the cached difference between the source server
// clock and this process's clock.
func (c *MongoConnector) getMongoClockOffset(ctx context.Context) (time.Duration, error) {
	if time.Since(c.clockOffsetUpdatedAt) < mongoClockOffsetTTL {
		return c.clockOffset, nil
	}
	offset, err := c.queryMongoClockOffset(ctx)
	if err != nil {
		return c.clockOffset, err
	}
	c.clockOffset = offset
	c.clockOffsetUpdatedAt = time.Now()
	return offset, nil
}

// SourceClockOffset implements connectors.SourceClockOffsetConnector.
func (c *MongoConnector) SourceClockOffset(ctx context.Context) (time.Duration, error) {
	return c.getMongoClockOffset(ctx)
}

// queryMongoClockOffset estimates the difference between the source server clock
// and this process's clock.
func (c *MongoConnector) queryMongoClockOffset(ctx context.Context) (time.Duration, error) {
	if c.client == nil {
		return 0, errors.New("MongoDB client is nil")
	}

	requestStarted := time.Now()
	var response struct {
		LocalTime time.Time `bson:"localTime"`
	}
	err := c.client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&response)
	responseReceived := time.Now()
	if err != nil {
		return 0, fmt.Errorf("failed to query MongoDB server time: %w", err)
	}
	if response.LocalTime.IsZero() {
		return 0, errors.New("MongoDB hello response did not include localTime")
	}

	return response.LocalTime.Sub(requestStarted.Add(responseReceived.Sub(requestStarted) / 2)), nil
}

// ChangeStream is defined as an interface, allowing tests inject mock change stream.
type ChangeStream interface {
	Next(ctx context.Context) bool
	ResumeToken() bson.Raw
	Err() error
	Close() error
	Current() bson.Raw
}

type changeStreamWrapper struct {
	*mongo.ChangeStream
}

func (w *changeStreamWrapper) Current() bson.Raw {
	return w.ChangeStream.Current
}

func (w *changeStreamWrapper) Close() error {
	// Intentionally not tied to the caller's context since Close often runs when
	// context is already canceled, preventing Close from executing a killCursors
	// command. This can lead to "Cannot open a new cursor since too many cursors
	// are already opened" error on DocumentDB (which caps cursor per instance),
	// as server's idle-cursor reaper can take time to kick in.
	closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return w.ChangeStream.Close(closeCtx)
}

func (c *MongoConnector) GetTableSchema(
	ctx context.Context,
	_ map[string]string,
	internalVersion uint32,
	_ protos.TypeSystem,
	tableMappings []*protos.TableMapping,
) (map[string]*protos.TableSchema, error) {
	result := make(map[string]*protos.TableSchema, len(tableMappings))
	idFieldDescription := &protos.FieldDescription{
		Name:         DefaultDocumentKeyColumnName,
		Type:         string(types.QValueKindString),
		TypeModifier: -1,
		Nullable:     false,
	}
	fullDocumentColumnName := DefaultFullDocumentColumnName
	if internalVersion < shared.InternalVersion_MongoDBFullDocumentColumnToDoc {
		fullDocumentColumnName = LegacyFullDocumentColumnName
	}
	dataFieldDescription := &protos.FieldDescription{
		Name:         fullDocumentColumnName,
		Type:         string(types.QValueKindJSON),
		TypeModifier: -1,
		Nullable:     false,
	}

	for _, tm := range tableMappings {
		result[tm.SourceTableIdentifier] = &protos.TableSchema{
			TableIdentifier:       tm.SourceTableIdentifier,
			PrimaryKeyColumns:     []string{DefaultDocumentKeyColumnName},
			IsReplicaIdentityFull: true,
			System:                protos.TypeSystem_Q,
			NullableEnabled:       false,
			Columns: []*protos.FieldDescription{
				idFieldDescription,
				dataFieldDescription,
			},
		}
	}

	return result, nil
}

func (c *MongoConnector) SetupReplication(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	input *protos.SetupReplicationInput,
) (model.SetupReplicationResult, error) {
	changeStreamOpts := options.ChangeStream().
		SetComment("PeerDB changeStream").
		SetFullDocument(options.UpdateLookup)

	pipeline, err := createPipeline(nil, nil)
	if err != nil {
		return model.SetupReplicationResult{}, fmt.Errorf("failed to create changestream pipeline: %w", err)
	}
	changeStream, err := c.createChangeStream(ctx, pipeline, changeStreamOpts)
	if err != nil {
		return model.SetupReplicationResult{}, fmt.Errorf("failed to start change stream for storing initial resume token: %w", err)
	}
	defer changeStream.Close()

	c.logger.Info("SetupReplication started, waiting for initial resume token")
	var resumeToken bson.Raw
	for {
		resumeToken = changeStream.ResumeToken()
		if resumeToken != nil {
			break
		} else {
			c.logger.Info("Resume token not available, waiting for next change event...")
			if !changeStream.Next(ctx) {
				return model.SetupReplicationResult{}, fmt.Errorf("change stream error: %w", changeStream.Err())
			}
		}
	}
	err = c.metadataStore.SetLastOffset(ctx, input.FlowJobName, model.CdcCheckpoint{
		Text: base64.StdEncoding.EncodeToString(resumeToken),
	})
	if err != nil {
		return model.SetupReplicationResult{}, fmt.Errorf("failed to store initial resume token: %w", err)
	}
	c.logger.Info("SetupReplication completed, stored initial resume token")
	return model.SetupReplicationResult{}, nil
}

// This function implements raw events decoding logic into typed `ChangeEvent` values.
func decodeEvent(
	rawEvent bson.Raw,
	changeEvent *ChangeEvent,
) error {
	if err := bson.Unmarshal(rawEvent, changeEvent); err != nil {
		return fmt.Errorf("failed to decode change stream document: %w", err)
	}
	return nil
}

// Constants used by PullRecords.
//
// Buffered channel size for channels to pass records to decode and send loops (see below).
// This should ideally be larger than decodeWorkerBufSize.
const workerBufferedChanSize = 10

// Number of recordItems to pass in one batch to decode/send loops. Doing per-item channel sends
// results in too much coordination and reduces effective concurrency in practice.
const pullRecordsItemsBatchSize = 256

// maxNumDecodeWorkers is the maximum number of goroutines that decodeLoop spins up to parallelize
// decoding of record batches.
const maxNumDecodeWorkers = 6

// decodeWorkerBufSize is the size of the channel to each decode worker spun up by decodeLoop.
const decodeWorkerBufSize = 4

// Amount of time to wait for workers to drain before timing out.
const workerDrainTimeout = 2 * time.Minute

type decodeBatch struct {
	// base64-encoded resume token
	resumeToken string
	items       []recordItems
}

type sendBatch struct {
	// base64-encoded resume token
	resumeToken string
	// records to send to RecordStream.
	records []model.Record[model.RecordItems]
}

// Two additional loops are created by PullRecords, each running in their own goroutines.
// One is decodeLoop, which takes records from the main PullRecords goroutine through `records`
// that have already had the operation type decoded, but not the full document. decodeLoop manages
// decodeWorkers running in their own goroutines that handle the actual bson document parsing. After
// a decodeWorker has parsed a batch of items, it passes the batch to sendLoop, which assembles records
// from all decodeWorkers into req.RecordStream. req.RecordStream.AddRecord could block on the downstream
// channel send, so separating `sendLoop` out this way from `decodeWorker` makes sense to speed up the
// relatively CPU-heavy `decodeWorker`. `sendLoop` is also responsible for advancing the resume token
// inside req.RecordStream, except in cases when `sendLoop` has been confirmed to be terminated,
// in which case `PullRecords` itself can do the advancement.
//
// decodeLoop is expected to propagate channel closures through to sendLoop for graceful
// draining to ensure no records are lost. Context cancellation is only used in the error
// path, where losing inflight records is acceptable with the expectation that a future
// restart from the last checkpoint will see those events again. Note that in case of
// recreation of the upstream changestream, we could drain and restart these two worker loops
// plus any decodeWorkers within the execution of one `PullRecords`.
func (c *MongoConnector) sendLoop(
	ctx context.Context,
	records <-chan chan sendBatch,
	errChan chan<- error,
	req *model.PullRecordsRequest[model.RecordItems],
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for {
		select {
		case recordChan, ok := <-records:
			if !ok {
				return
			}
			select {
			case sendBatch := <-recordChan:
				for i := range sendBatch.records {
					if err := req.RecordStream.AddRecord(ctx, sendBatch.records[i]); err != nil {
						select {
						case errChan <- err:
						case <-ctx.Done():
						}
						return
					}
				}
				if sendBatch.resumeToken != "" {
					req.RecordStream.UpdateLatestCheckpointText(sendBatch.resumeToken)
				}
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

type recordItems struct {
	maybeFullDocument    *bson.Raw
	operationType        operationType
	sourceTableName      string
	destinationTableName string
	documentKey          bson.Raw
	commitTimeNanos      int64
}

// decodeWorker is spun up by decodeLoop in separate goroutines, up to
// maxNumDecodeWorker in parallel. Batches of items to decode are passed in
// through recv, and the output is sent through `send` to `sendLoop` directly.
func (c *MongoConnector) decodeWorker(
	ctx context.Context,
	recv <-chan decodeBatch,
	send chan<- sendBatch,
	req *model.PullRecordsRequest[model.RecordItems],
) error {
	// Utils used by this routine.
	converter := NewDirectBsonConverter()
	fullDocumentColumnName := DefaultFullDocumentColumnName
	if req.InternalVersion < shared.InternalVersion_MongoDBFullDocumentColumnToDoc {
		fullDocumentColumnName = LegacyFullDocumentColumnName
	}
	parseItem := func(item recordItems) (model.Record[model.RecordItems], error) {
		items := model.NewRecordItems(2)

		if len(item.documentKey) > 0 {
			rv := item.documentKey.Lookup(DefaultDocumentKeyColumnName)
			if rv.IsZero() || rv.Type == bson.TypeNull {
				return nil, exceptions.NewInvalidIdValueError(item.sourceTableName)
			}
			qValue, err := converter.QValueStringFromId(rv, req.InternalVersion)
			if err != nil {
				return nil, err
			}
			items.AddColumn(DefaultDocumentKeyColumnName, qValue)
		} else {
			return nil, fmt.Errorf("document key is nil")
		}

		if item.maybeFullDocument != nil && len(*item.maybeFullDocument) > 0 {
			qValue, err := converter.QValueJSONFromDocument(*item.maybeFullDocument)
			if err != nil {
				return nil, fmt.Errorf("failed to convert document: %w", err)
			}
			items.AddColumn(fullDocumentColumnName, qValue)
		} else {
			// `fullDocument` field will not exist in the following scenarios:
			// 1) operationType is 'delete'
			// 2) document is deleted / collection is dropped in between update and lookup
			// 3) update changes the values for at least one of the fields in that collection's
			//    shard key (although sharding is not supported today)
			items.AddColumn(fullDocumentColumnName, types.QValueJSON{Val: "{}"})
		}
		var record model.Record[model.RecordItems]
		switch item.operationType {
		case operationTypeInsert:
			record = &model.InsertRecord[model.RecordItems]{
				BaseRecord:           model.BaseRecord{CommitTimeNano: item.commitTimeNanos},
				Items:                items,
				SourceTableName:      item.sourceTableName,
				DestinationTableName: item.destinationTableName,
			}

		case operationTypeUpdate, operationTypeReplace:
			record = &model.UpdateRecord[model.RecordItems]{
				BaseRecord:           model.BaseRecord{CommitTimeNano: item.commitTimeNanos},
				NewItems:             items,
				SourceTableName:      item.sourceTableName,
				DestinationTableName: item.destinationTableName,
			}
		case operationTypeDelete:
			record = &model.DeleteRecord[model.RecordItems]{
				BaseRecord:           model.BaseRecord{CommitTimeNano: item.commitTimeNanos},
				Items:                items,
				SourceTableName:      item.sourceTableName,
				DestinationTableName: item.destinationTableName,
			}
		}
		return record, nil
	}
	for {
		select {
		case batch, ok := <-recv:
			if !ok {
				return nil
			}
			items := batch.items

			modelRecords := make([]model.Record[model.RecordItems], len(items))
			for i := range items {
				var err error
				if modelRecords[i], err = parseItem(items[i]); err != nil {
					return err
				}
			}

			select {
			case send <- sendBatch{records: modelRecords, resumeToken: batch.resumeToken}:
			case <-ctx.Done():
				return ctx.Err()
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *MongoConnector) decodeLoop(
	parentCtx context.Context,
	recv <-chan decodeBatch,
	errChan chan<- error,
	req *model.PullRecordsRequest[model.RecordItems],
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	// NB: child workers are tracked by this errGroup, not by
	// the parent WaitGroup (shared with PullRecords and sendLoop).
	eg, ctx := errgroup.WithContext(parentCtx)
	defer func() {
		err := eg.Wait()
		if err != nil {
			select {
			case errChan <- err:
			case <-parentCtx.Done():
			}
		}
	}()

	// This function spins up half as many workers as the number of cores, capped
	// at maxNumDecodeWorkers.
	numWorkers := max(1, min(maxNumDecodeWorkers, runtime.GOMAXPROCS(0)/2))
	workerChan := make([]struct {
		req chan decodeBatch
		res chan sendBatch
	}, numWorkers)
	for i := range workerChan {
		workerChan[i].req = make(chan decodeBatch, decodeWorkerBufSize)
		workerChan[i].res = make(chan sendBatch, decodeWorkerBufSize)
		// Start worker.
		eg.Go(func() error {
			return c.decodeWorker(ctx, workerChan[i].req, workerChan[i].res, req)
		})
	}

	// Start up sendLoop. Have a buffered channel in case decoding runs faster
	// than the downstream addition of records to req.RecordStream.
	sender := make(chan chan sendBatch, decodeWorkerBufSize*maxNumDecodeWorkers)
	go c.sendLoop(parentCtx, sender, errChan, req, wg)
	// Index of next worker to send a result to.
	nextWorker := 0

	for {
		select {
		case item, ok := <-recv:
			if !ok {
				// Draining. Close all worker channels.
				for i := range workerChan {
					close(workerChan[i].req)
				}
				close(sender)
				return
			}

			select {
			case workerChan[nextWorker].req <- item:
				sender <- workerChan[nextWorker].res
				nextWorker = (nextWorker + 1) % numWorkers
			case <-ctx.Done():
				return
			}
		case <-parentCtx.Done():
			// Context cancellation usually means an error, not just cutting a batch (which would be
			// a graceful drain via recv getting closed).
			return
		}
	}
}

func (c *MongoConnector) PullRecords(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	otelManager *otel_metrics.OtelManager,
	req *model.PullRecordsRequest[model.RecordItems],
) (retErr error) {
	defer req.RecordStream.Close()

	var alerter *alerting.Alerter
	if catalogPool.Pool != nil {
		alerter = alerting.NewAlerter(ctx, catalogPool, otelManager)
	}

	fullDocumentColumnName := DefaultFullDocumentColumnName
	if req.InternalVersion < shared.InternalVersion_MongoDBFullDocumentColumnToDoc {
		fullDocumentColumnName = LegacyFullDocumentColumnName
	}
	var wg sync.WaitGroup

	c.logger.Info("[mongo] started PullRecords for mirror "+req.FlowJobName,
		slog.Any("table_mapping", req.TableNameMapping),
		slog.Uint64("max_batch_size", uint64(req.MaxBatchSize)),
		slog.Duration("sync_interval", req.IdleTimeout))

	changeStreamOpts := options.ChangeStream().
		SetComment("PeerDB changeStream for mirror " + req.FlowJobName).
		SetFullDocument(options.UpdateLookup).
		// batchSize=0 only affects the initial aggregate response, so Watch returns
		// immediately without blocking on the initial cursor establishment. Subsequent
		// getMore calls fall back to the server default (up to 16 MiB per batch).
		// https://www.mongodb.com/docs/manual/reference/method/cursor.batchSize/
		SetBatchSize(0)

	var resumeToken bson.Raw
	var err error
	mongoClockOffset, err := c.getMongoClockOffset(ctx)
	if err != nil {
		c.logger.Warn("failed to calculate MongoDB clock offset", slog.Any("error", err))
	}

	if req.LastOffset.Text != "" {
		// If we have a last offset, we resume from that point
		c.logger.Info("[mongo] resuming change stream", slog.String("resumeToken", req.LastOffset.Text))
		resumeToken, err = base64.StdEncoding.DecodeString(req.LastOffset.Text)
		if err != nil {
			return fmt.Errorf("failed to parse last offset: %w", err)
		}
		changeStreamOpts.SetResumeAfter(resumeToken)
	}

	pipeline, err := createPipeline(req.TableNameMapping, c.excludedOps)
	if err != nil {
		return err
	}

	changeStream, err := c.createChangeStream(ctx, pipeline, changeStreamOpts)
	if err != nil {
		if isResumeTokenNotFoundError(err) && resumeToken != nil {
			timestamp, err := decodeTimestampFromResumeToken(resumeToken)
			if err != nil {
				return fmt.Errorf("failed to decode resume token: %w", err)
			}
			changeStreamOpts.SetStartAtOperationTime(&timestamp)
			changeStreamOpts.SetResumeAfter(nil)
			changeStream, err = c.createChangeStream(ctx, pipeline, changeStreamOpts)
			if err != nil {
				return fmt.Errorf("failed to recreate change stream: %w", err)
			}
		} else {
			return fmt.Errorf("failed to create change stream: %w", err)
		}
	}
	defer func() {
		// Wrapped in a closure so changeStream is evaluated at return time. A direct
		// `defer changeStream.Close()` would bind the original stream created above
		// and miss any replacement made by recreateChangeStream.
		if changeStream != nil {
			if err := changeStream.Close(); err != nil {
				c.logger.Warn("failed to close change stream", slog.Any("error", err))
			}
		}
	}()

	var recordCount uint32
	var deltaBytesProcessed, cumulativeBytesProcessed atomic.Int64
	pullStart := time.Now()
	defer func() {
		if recordCount == 0 {
			req.RecordStream.SignalAsEmpty()
		}
		span := trace.SpanFromContext(ctx)
		span.SetAttributes(
			attribute.Int64(otel_metrics.RowsInBatchKey, int64(recordCount)),
			attribute.Int64(otel_metrics.BytesPulledKey, cumulativeBytesProcessed.Load()),
		)
		if changeStream != nil && retErr == nil {
			// NB: We don't set the otel metrics ResumeTokenKey in the error case because
			// it's possible for the changeStream to have been advanced past the last record
			// send to RecordStream (and therefore the actual resume token set in RecordStream).
			// It's safer to not report a resume token than to report an incorrect one that can
			// skip records if used.
			if rt := changeStream.ResumeToken(); rt != nil {
				rtStr := base64.StdEncoding.EncodeToString(rt)
				if len(rtStr) > 64 {
					rtStr = rtStr[:64]
				}
				span.SetAttributes(attribute.String(otel_metrics.ResumeTokenKey, rtStr))
			}
		}
		c.logger.Info("[mongo] PullRecords finished streaming",
			slog.Uint64("records", uint64(recordCount)),
			slog.Int64("bytes", cumulativeBytesProcessed.Load()),
			slog.Int("channelLen", req.RecordStream.ChannelLen()),
			slog.Float64("elapsedMinutes", time.Since(pullStart).Minutes()))
	}()
	// Context inheritance tree: There's a parent ctx (always called ctx in this function).
	// Off of that, we create two child contexts, one for workers (called workerCtx), that's passed
	// to children workers (eg. decodeLoop). The other, timeoutCtx, is managed by us for timeouts.
	// Note that when we hit a timeout, we don't want to cancel the workerCtx; we want the workers
	// to gracefully drain.
	//
	// Also note that timeoutCtx is occasionally rewritten, such as when a record in a batch arrives
	// or when we reset the changestream.
	workerCtx, workerCtxCancel := context.WithCancel(ctx)
	// before the first record arrives, we wait for up to an hour before resetting context timeout
	// after the first record arrives, we switch to configured idleTimeout
	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, time.Hour)

	reportBytesShutdown := common.Interval(ctx, time.Second*10, func() {
		read := deltaBytesProcessed.Swap(0)
		otelManager.Metrics.FetchedBytesCounter.Add(ctx, read)
		otelManager.Metrics.AllFetchedBytesCounter.Add(ctx, read)
	})

	defer func() {
		cancelTimeout()
		workerCtxCancel()
		wg.Wait()
		reportBytesShutdown()
		read := deltaBytesProcessed.Swap(0)
		otelManager.Metrics.FetchedBytesCounter.Add(ctx, read)
		otelManager.Metrics.AllFetchedBytesCounter.Add(ctx, read)
	}()

	checkpoint := func() string {
		rt := changeStream.ResumeToken()
		if rt == nil {
			c.logger.Warn("change stream does not currently contain a resume token")
			return ""
		}
		text := base64.StdEncoding.EncodeToString(rt)
		req.RecordStream.UpdateLatestCheckpointText(text)
		return text
	}
	checkpointToCatalog := func() {
		text := checkpoint()
		if text == "" {
			return
		}
		if err := c.metadataStore.SetLastOffset(ctx, req.FlowJobName, model.CdcCheckpoint{Text: text}); err != nil {
			c.logger.Error("failed to persist resume token", slog.String("resumeToken", text), slog.Any("error", err))
		}
	}

	incrementRecordCount := func() {
		recordCount += 1
		if recordCount == 1 {
			req.RecordStream.SignalAsNotEmpty()
			timeoutCtx, cancelTimeout = context.WithTimeout(ctx, req.IdleTimeout) //nolint:gosec // G118: cancelTimeout called in defer
		}
		if recordCount%50000 == 0 {
			c.logger.Info("[mongo] PullRecords streaming",
				slog.Uint64("records", uint64(recordCount)),
				slog.Int64("bytes", cumulativeBytesProcessed.Load()),
				slog.Int("channelLen", req.RecordStream.ChannelLen()),
				slog.Float64("elapsedMinutes", time.Since(pullStart).Minutes()))
		}
	}
	existingBatch := make([]recordItems, 0, pullRecordsItemsBatchSize)
	decodeChan := make(chan decodeBatch, workerBufferedChanSize)
	errChan := make(chan error, 2) // size = one for decodeLoop, one for sendLoop.
	// decodeLoop starts up sendLoop.
	wg.Add(2)
	go c.decodeLoop(workerCtx, decodeChan, errChan, req, &wg)

	finishBatch := func() error {
		if len(existingBatch) == 0 {
			// Nothing to send.
			return nil
		}
		rt := changeStream.ResumeToken()
		var rtText string
		if rt == nil {
			c.logger.Warn("change stream does not currently contain a resume token")
		} else {
			rtText = base64.StdEncoding.EncodeToString(rt)
		}
		select {
		case decodeChan <- decodeBatch{items: existingBatch, resumeToken: rtText}:
		case err := <-errChan:
			workerCtxCancel()
			return err
		case <-ctx.Done():
			workerCtxCancel()
			return ctx.Err()
		}
		existingBatch = make([]recordItems, 0, pullRecordsItemsBatchSize)
		return nil
	}

	drainWorkers := func() error {
		finishErr := finishBatch()
		// Close the decode loop and wait for events to drain.
		close(decodeChan)
		wgWaiter := make(chan bool)
		go func() {
			wg.Wait()
			close(wgWaiter)
		}()
		select {
		case <-wgWaiter:
			// Check if there was an error returned at the same time
			// as the other goroutines disappeared.
			select {
			case err := <-errChan:
				return err
			default:
			}
		case err := <-errChan:
			workerCtxCancel()
			<-wgWaiter
			return err
		case <-time.After(workerDrainTimeout):
			workerCtxCancel()
			<-wgWaiter
			return errors.New("timed out waiting for PullRecords workers to drain")
		}
		return finishErr
	}

	recreateChangeStream := func(useOperationTime bool) error {
		// extract the most recent resumeToken
		resumeToken := changeStream.ResumeToken()
		if resumeToken == nil {
			return fmt.Errorf("resume token is nil")
		}

		// close existing change stream
		if err := changeStream.Close(); err != nil {
			return fmt.Errorf("failed to close change stream: %w", err)
		}

		// reset context timeout
		cancelTimeout()
		timeoutCtx, cancelTimeout = context.WithTimeout(ctx, time.Hour)

		// Restart the decode loop
		decodeChan = make(chan decodeBatch, workerBufferedChanSize)
		// decodeLoop starts up sendLoop; add one for each.
		wg.Add(2)
		go c.decodeLoop(workerCtx, decodeChan, errChan, req, &wg)

		// set resume point based on whether operation time should be used or not
		if useOperationTime {
			timestamp, err := decodeTimestampFromResumeToken(resumeToken)
			if err != nil {
				return fmt.Errorf("failed to decode resume token: %w", err)
			}
			changeStreamOpts.SetStartAtOperationTime(&timestamp)
			changeStreamOpts.SetResumeAfter(nil)
		} else {
			changeStreamOpts.SetResumeAfter(resumeToken)
			changeStreamOpts.SetStartAtOperationTime(nil)
		}

		changeStream, err = c.createChangeStream(ctx, pipeline, changeStreamOpts)
		if err != nil {
			return err
		}

		return nil
	}

	var lastEventGaugesRecordedAt time.Time
	for recordCount < req.MaxBatchSize {
		if ok := changeStream.Next(timeoutCtx); !ok {
			err := changeStream.Err()
			if err == nil {
				return fmt.Errorf("unexpected: changestream.Next() returned false but no change stream error was recorded")
			}

			// Before checking for timeout, see if there's an error from the child workers waiting.
			select {
			case err := <-errChan:
				workerCtxCancel()
				return err
			default:
			}
			if err := drainWorkers(); err != nil {
				return err
			}

			if errors.Is(err, context.DeadlineExceeded) {
				if recordCount > 0 {
					// advance offset to the PostBatchResumeToken since the last change event's resume token may be quite old
					//
					// This checkpoint is safe to do here as opposed to in sendLoop, because sendLoop has been drained away
					// above.
					checkpoint()
					return nil
				}
				// when no events arrived in this batch, still advance offset to the PostBatchResumeToken.
				// it's safe to persist to catalog since no records were handed off to the sync workflow,
				// so there's no in-flight data we could skip past by advancing the offset.
				checkpointToCatalog()
				// DeadlineExceeded errors are deemed not recoverable/resumable, so we have to create a new change stream instance
				if err := recreateChangeStream(false); err != nil {
					return fmt.Errorf("failed to recreate change stream: %w", err)
				}
				c.logger.Info("[mongo] recreated change stream because context deadline exceeded",
					slog.Duration("elapsed", time.Since(pullStart)))
				continue
			}

			if isResumeTokenNotFoundError(err) {
				if err := recreateChangeStream(true); err != nil {
					return fmt.Errorf("failed to recreate change stream: %w", err)
				}
				c.logger.Info("[mongo] recreated change stream because resume token not found", slog.Duration("elapsed", time.Since(pullStart)))
				continue
			}

			return fmt.Errorf("change stream error: %w", err)
		}

		current := changeStream.Current()
		changeEventSize := int64(len(current))
		deltaBytesProcessed.Add(changeEventSize)
		cumulativeBytesProcessed.Add(changeEventSize)

		var changeEvent ChangeEvent
		if err := decodeEvent(current, &changeEvent); err != nil {
			return err
		}

		clusterTime := time.Unix(int64(changeEvent.ClusterTime.T), 0)
		commitTime := clusterTime
		if changeEvent.WallTime != nil {
			// wallTime (MongoDB 6+) is the source server's wall-clock operation time;
			// clusterTime is an oplog timestamp used primarily for ordering and is only second-resolution.
			commitTime = changeEvent.WallTime.UTC()
		}
		commitTimeNanos := commitTime.UnixNano()

		if time.Since(lastEventGaugesRecordedAt) >= time.Second {
			// recording gauges per event is wasteful on CPU given this is on the hot path
			otelManager.Metrics.LatestConsumedLogEventGauge.Record(ctx, clusterTime.Unix())
			otelManager.Metrics.SourceLagGauge.Record(ctx,
				time.Now().UTC().Add(mongoClockOffset).Sub(commitTime).Milliseconds())
			lastEventGaugesRecordedAt = time.Now()
		}

		sourceTableName := fmt.Sprintf("%s.%s", changeEvent.Ns.Db, changeEvent.Ns.Coll)
		destinationTableName := req.TableNameMapping[sourceTableName].Name
		if destinationTableName == "" {
			// should never happen since pipeline should filter out irrelevant tables
			c.logger.Warn("Skipping event that cannot be mapped to a destination table %s", sourceTableName)
			continue
		}

		items := recordItems{
			documentKey:          changeEvent.DocumentKey,
			maybeFullDocument:    changeEvent.FullDocument,
			operationType:        operationType(changeEvent.OperationType),
			sourceTableName:      sourceTableName,
			destinationTableName: destinationTableName,
			commitTimeNanos:      commitTimeNanos,
		}
		switch items.operationType {
		case operationTypeInsert, operationTypeReplace, operationTypeUpdate, operationTypeDelete:
			// Happy path.
			incrementRecordCount()
		default:
			c.logger.Warn(fmt.Sprintf("skipping event with unsupported operation type '%s' (db=%s coll=%s)",
				changeEvent.OperationType, changeEvent.Ns.Db, changeEvent.Ns.Coll))

			// When the skipped event is a collection-level DDL (drop or rename), generate customer facing logs.
			if alerter != nil {
				switch operationType(changeEvent.OperationType) {
				case operationTypeDrop, operationTypeRename:
					ddlErr := fmt.Errorf(
						"%s event on %s.%s: collection DDL is not replicated, the destination table is left unchanged",
						changeEvent.OperationType, changeEvent.Ns.Db, changeEvent.Ns.Coll)
					alerter.LogFlowWarning(ctx, req.FlowJobName, ddlErr)
				}
			} else {
				c.logger.Error("Alerter not initialized")
			}

			continue
		}
		otelManager.Metrics.FetchedEventSizeHistogram.Record(ctx, changeEventSize)
		existingBatch = append(existingBatch, items)
		if len(existingBatch) >= pullRecordsItemsBatchSize {
			if err := finishBatch(); err != nil {
				return err
			}
		}
	}

	if err := drainWorkers(); err != nil {
		return err
	}

	return nil
}

func createPipeline(tableNameMapping map[string]model.NameAndExclude, excludedOps []operationType) (mongo.Pipeline, error) {
	pipeline := mongo.Pipeline{}

	// filter out events from tables that are not in the mapping
	if tableNameMapping != nil {
		dbCollMap := make(map[string][]string)
		for dbAndTable := range tableNameMapping {
			parts := strings.SplitN(dbAndTable, ".", 2)
			if len(parts) != 2 {
				return nil, fmt.Errorf("failed to create pipeline due to invalid table name: %s", dbAndTable)
			}
			db := parts[0]
			table := parts[1]
			dbCollMap[db] = append(dbCollMap[db], table)
		}

		var orCondition bson.A
		for db, tables := range dbCollMap {
			andCondition := bson.A{
				bson.D{{Key: "ns.db", Value: db}},
				bson.D{{Key: "ns.coll", Value: bson.D{{Key: "$in", Value: tables}}}},
			}
			orCondition = append(orCondition, bson.D{
				{Key: "$and", Value: andCondition},
			})
		}

		pipeline = append(pipeline, bson.D{{Key: "$match", Value: bson.D{
			{Key: "$or", Value: orCondition},
		}}})
	}

	// filter out excluded operation types
	if len(excludedOps) > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$match", Value: bson.D{
			{Key: "operationType", Value: bson.D{{Key: "$nin", Value: excludedOps}}},
		}}})
	}

	// Mongo recommends using '$project' first to reduce change event size, and only use
	// '$changeStreamSplitLargeEvent' in the pipeline if still necessary. Given the document
	// themselves have a 16MB limit, project required fields for now for code simplicity.
	// ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/changeStreamSplitLargeEvent/
	pipeline = append(pipeline,
		bson.D{{Key: "$project", Value: bson.D{
			{Key: "operationType", Value: 1},
			{Key: "clusterTime", Value: 1},
			{Key: "wallTime", Value: 1},
			{Key: "documentKey", Value: 1},
			{Key: "fullDocument", Value: 1},
			{Key: "ns", Value: 1},
		}}},
	)

	return pipeline, nil
}

// This can happen if the resumeToken we are attempting to `ResumeAfter` refers to a table that has been
// filtered out of the change stream pipeline (for example, if a user pauses and edits a mirror). If
// this happens, we decode the resumeToken and extract its operation time, and start a new changeStream
// with `StartAtOperationTime` instead of `ResumeAfter`.
func isResumeTokenNotFoundError(err error) bool {
	return strings.Contains(err.Error(), "cannot resume stream; the resume token was not found.")
}

// stubs for CDCPullConnectorCore

func (c *MongoConnector) EnsurePullability(ctx context.Context, req *protos.EnsurePullabilityBatchInput) (
	*protos.EnsurePullabilityBatchOutput, error,
) {
	return nil, nil
}

func (c *MongoConnector) ExportTxSnapshot(context.Context, string, map[string]string) (*protos.ExportTxSnapshotOutput, any, error) {
	return nil, nil, nil
}

func (c *MongoConnector) FinishExport(any) error {
	return nil
}

func (c *MongoConnector) SetupReplConn(ctx context.Context, env map[string]string) error {
	// Unlike Postgres, MongoDB doesn't need a dedicated replication connection:
	// change streams are cursors served by the connector's pooled client.
	// Since SetupReplConn is called once per SyncFlow activity, resolving
	// dynamic config here avoids per-batch catalog reads.
	excludedOps, err := internal.PeerDBMongoDBExcludedOperationTypes(ctx, env)
	if err != nil {
		return fmt.Errorf("failed to get excluded operation types: %w", err)
	}
	c.excludedOps = make([]operationType, 0, len(excludedOps))
	for _, op := range excludedOps {
		if parsed, ok := parseOperationType(op); ok {
			if !slices.Contains(c.excludedOps, parsed) {
				c.excludedOps = append(c.excludedOps, parsed)
			}
		} else {
			c.logger.Warn("ignoring invalid operation type in exclusion list", slog.String("operationType", op))
		}
	}
	if len(c.excludedOps) > 0 {
		c.logger.Info("excluding operation types from replication", slog.Any("operationTypes", c.excludedOps))
	}
	return nil
}

func (c *MongoConnector) UpdateReplStateLastOffset(ctx context.Context, lastOffset model.CdcCheckpoint) error {
	return nil
}

func (c *MongoConnector) PullFlowCleanup(ctx context.Context, jobName string) error {
	return nil
}

// end stubs
