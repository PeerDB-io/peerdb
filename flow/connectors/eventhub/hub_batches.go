package conneventhub

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	azeventhubs "github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	cmap "github.com/orcaman/concurrent-map/v2"
	log "github.com/sirupsen/logrus"
)

// multimap from ScopedEventhub to *azeventhubs.EventDataBatch
type HubBatches struct {
	batches map[ScopedEventhub][]*azeventhubs.EventDataBatch
	manager *EventHubManager
}

func NewHubBatches(manager *EventHubManager) *HubBatches {
	return &HubBatches{
		batches: make(map[ScopedEventhub][]*azeventhubs.EventDataBatch),
		manager: manager,
	}
}

func (h *HubBatches) AddEvent(ctx context.Context, name ScopedEventhub, event string) error {
	batches, ok := h.batches[name]
	if !ok {
		batches = []*azeventhubs.EventDataBatch{}
	}

	if len(batches) == 0 {
		newBatch, err := h.manager.CreateEventDataBatch(ctx, name)
		if err != nil {
			return err
		}
		batches = append(batches, newBatch)
	}

	if err := tryAddEventToBatch(event, batches[len(batches)-1]); err != nil {
		if strings.Contains(err.Error(), "too large for the batch") {
			overflowBatch, err := h.handleBatchOverflow(ctx, name, event)
			if err != nil {
				return fmt.Errorf("failed to handle batch overflow: %v", err)
			}
			batches = append(batches, overflowBatch)
		} else {
			return fmt.Errorf("failed to add event data: %v", err)
		}
	}

	h.batches[name] = batches
	return nil
}

func (h *HubBatches) handleBatchOverflow(
	ctx context.Context,
	name ScopedEventhub,
	event string,
) (*azeventhubs.EventDataBatch, error) {
	newBatch, err := h.manager.CreateEventDataBatch(ctx, name)
	if err != nil {
		return nil, err
	}
	if err := tryAddEventToBatch(event, newBatch); err != nil {
		return nil, fmt.Errorf("failed to add event data to new batch: %v", err)
	}
	return newBatch, nil
}

func (h *HubBatches) Len() int {
	return len(h.batches)
}

// ForEach calls the given function for each ScopedEventhub and batch pair
func (h *HubBatches) ForEach(fn func(ScopedEventhub, *azeventhubs.EventDataBatch)) {
	for name, batches := range h.batches {
		for _, batch := range batches {
			fn(name, batch)
		}
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

	log.Infof("successfully sent %d events to event hub topic - %s", events.NumEvents(), tblName.ToString())
	return nil
}

func (h *HubBatches) flushAllBatches(
	ctx context.Context,
	events *HubBatches,
	maxParallelism int64,
	flowName string,
	tableNameRowsMapping cmap.ConcurrentMap[string, uint32]) error {
	if events.Len() == 0 {
		log.WithFields(log.Fields{
			"flowName": flowName,
		}).Infof("no events to send")
		return nil
	}

	var numEventsPushed int32
	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error
	// Limiting concurrent sends
	guard := make(chan struct{}, maxParallelism)

	events.ForEach(func(tblName ScopedEventhub, eventBatch *azeventhubs.EventDataBatch) {
		guard <- struct{}{}
		wg.Add(1)
		go func(tblName ScopedEventhub, eventBatch *azeventhubs.EventDataBatch) {
			defer func() {
				<-guard
				wg.Done()
			}()

			numEvents := eventBatch.NumEvents()
			err := h.sendBatch(ctx, tblName, eventBatch)
			if err != nil {
				once.Do(func() { firstErr = err })
				return
			}

			atomic.AddInt32(&numEventsPushed, numEvents)
			log.WithFields(log.Fields{
				"flowName": flowName,
			}).Infof("pushed %d events to event hub: %s", numEvents, tblName)
			rowCount, ok := tableNameRowsMapping.Get(tblName.ToString())
			if !ok {
				rowCount = uint32(0)
			}
			rowCount += uint32(numEvents)
			tableNameRowsMapping.Set(tblName.ToString(), rowCount)
		}(tblName, eventBatch)
	})

	wg.Wait()

	if firstErr != nil {
		log.Error(firstErr)
		return firstErr
	}

	log.Infof("[sendEventBatch] successfully sent %d events to event hub", numEventsPushed)
	return nil
}

// Clear removes all batches from the HubBatches
func (h *HubBatches) Clear() {
	h.batches = make(map[ScopedEventhub][]*azeventhubs.EventDataBatch)
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
