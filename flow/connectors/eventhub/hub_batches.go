package conneventhub

import (
	"context"
	"fmt"
	"strings"

	azeventhubs "github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
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
