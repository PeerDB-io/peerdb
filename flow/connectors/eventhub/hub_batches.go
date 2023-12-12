package conneventhub

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	azeventhubs "github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/PeerDB-io/peer-flow/shared"
	"golang.org/x/sync/errgroup"
)

// multimap from ScopedEventhub to *azeventhubs.EventDataBatch
type HubBatches struct {
	batch   map[ScopedEventhub]*azeventhubs.EventDataBatch
	manager *EventHubManager
}

func NewHubBatches(manager *EventHubManager) *HubBatches {
	return &HubBatches{
		batch:   make(map[ScopedEventhub]*azeventhubs.EventDataBatch),
		manager: manager,
	}
}

func (h *HubBatches) AddEvent(
	ctx context.Context,
	name ScopedEventhub,
	event string,
	// this is true when we are retrying to send the event after the batch size exceeded
	// this should initially be false, and then true when we are retrying.
	retryForBatchSizeExceed bool,
) error {
	batch, ok := h.batch[name]
	if !ok || batch == nil {
		newBatch, err := h.manager.CreateEventDataBatch(ctx, name)
		if err != nil {
			return fmt.Errorf("failed to create event data batch: %v", err)
		}
		batch = newBatch
		h.batch[name] = batch
	}

	err := tryAddEventToBatch(event, batch)
	if err == nil {
		// we successfully added the event to the batch, so we're done.
		return nil
	}

	if errors.Is(err, azeventhubs.ErrEventDataTooLarge) {
		if retryForBatchSizeExceed {
			// if we are already retrying, then we should just return the error
			// as we have already tried to send the event to the batch.
			return fmt.Errorf("[retry-failed] event too large to add to batch: %v", err)
		}

		// if the event is too large, send the current batch and
		// delete it from the map, so that a new batch can be created
		// for the event next time.
		if err := h.sendBatch(ctx, name, batch); err != nil {
			return fmt.Errorf("failed to send batch: %v", err)
		}
		delete(h.batch, name)

		return h.AddEvent(ctx, name, event, true)
	} else {
		return fmt.Errorf("failed to add event to batch: %v", err)
	}
}

func (h *HubBatches) Len() int {
	return len(h.batch)
}

// ForEach calls the given function for each ScopedEventhub and batch pair
func (h *HubBatches) ForEach(fn func(ScopedEventhub, *azeventhubs.EventDataBatch)) {
	for name, batch := range h.batch {
		fn(name, batch)
	}
}

func (h *HubBatches) sendBatch(
	ctx context.Context,
	tblName ScopedEventhub,
	events *azeventhubs.EventDataBatch,
) error {
	subCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	hub, err := h.manager.GetOrCreateHubClient(subCtx, tblName)
	if err != nil {
		return err
	}

	opts := &azeventhubs.SendEventDataBatchOptions{}
	err = hub.SendEventDataBatch(subCtx, events, opts)
	if err != nil {
		return err
	}

	slog.InfoContext(ctx, "sendBatch",
		slog.Int("events sent", int(events.NumEvents())), slog.String("event hub topic ", tblName.ToString()))
	return nil
}

func (h *HubBatches) flushAllBatches(
	ctx context.Context,
	maxParallelism int64,
	flowName string,
) error {
	if h.Len() == 0 {
		slog.Info("no events to send", slog.String(string(shared.FlowNameKey), flowName))
		return nil
	}

	var numEventsPushed int32
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(int(maxParallelism))
	h.ForEach(func(tblName ScopedEventhub, eventBatch *azeventhubs.EventDataBatch) {
		g.Go(func() error {
			numEvents := eventBatch.NumEvents()
			err := h.sendBatch(gCtx, tblName, eventBatch)
			if err != nil {
				return err
			}

			atomic.AddInt32(&numEventsPushed, numEvents)
			slog.Info("flushAllBatches",
				slog.String(string(shared.FlowNameKey), flowName),
				slog.Int("events sent", int(numEvents)),
				slog.String("event hub topic ", tblName.ToString()))
			return nil
		})
	})

	err := g.Wait()
	slog.Info("hub batches flush",
		slog.String(string(shared.FlowNameKey), flowName),
		slog.Int("events sent", int(numEventsPushed)))

	// clear the batches after flushing them.
	h.Clear()

	return err
}

// Clear removes all batches from the HubBatches
func (h *HubBatches) Clear() {
	h.batch = make(map[ScopedEventhub]*azeventhubs.EventDataBatch)
}

func tryAddEventToBatch(event string, batch *azeventhubs.EventDataBatch) error {
	eventData := eventDataFromString(event)
	opts := &azeventhubs.AddEventDataOptions{}
	return batch.AddEventData(eventData, opts)
}

func eventDataFromString(s string) *azeventhubs.EventData {
	return &azeventhubs.EventData{
		Body: []byte(s),
	}
}
