package conneventhub

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	azeventhubs "github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	log "github.com/sirupsen/logrus"
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
func (h *HubBatches) ForEach(fn func(ScopedEventhub, *azeventhubs.EventDataBatch) error) error {
	for name, batch := range h.batch {
		err := fn(name, batch)
		if err != nil {
			return err
		}
	}
	return nil
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

	log.Infof("successfully sent %d events to event hub topic - %s", events.NumEvents(), tblName.ToString())
	return nil
}

func (h *HubBatches) flushAllBatches(
	ctx context.Context,
	flowName string,
) error {
	if h.Len() == 0 {
		log.WithFields(log.Fields{
			"flowName": flowName,
		}).Infof("no events to send")
		return nil
	}

	var numEventsPushed int32
	err := h.ForEach(
		func(
			tblName ScopedEventhub,
			eventBatch *azeventhubs.EventDataBatch,
		) error {
			numEvents := eventBatch.NumEvents()
			err := h.sendBatch(ctx, tblName, eventBatch)
			if err != nil {
				return err
			}

			atomic.AddInt32(&numEventsPushed, numEvents)
			log.WithFields(log.Fields{
				"flowName": flowName,
			}).Infof("pushed %d events to event hub: %s", numEvents, tblName)
			return nil
		})

	// clear the batches after flushing them.
	h.Clear()

	if err != nil {
		return fmt.Errorf("failed to flushAllBatches: %v", err)
	}

	log.Infof("[flush] successfully sent %d events in total to event hub",
		numEventsPushed)

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
