package connmongo

import (
	"encoding/json"

	"github.com/PeerDB-io/peerdb/flow/model"
)

// A stream belongs to one SyncFlow activity, whose pulls and acknowledgments are
// sequential. Only a successfully drained and acknowledged batch may reuse it.
// The driver's unread buffered events stay on the stream for the next batch.
type retainedChangeStream struct {
	stream       ChangeStream
	identity     string
	checkpoint   string
	acknowledged bool
}

func (c *MongoConnector) changeStreamIdentity(req *model.PullRecordsRequest[model.RecordItems]) (string, error) {
	// JSON sorts map keys. The pipeline itself is built from maps, so comparing
	// its BSON would reopen equivalent streams depending on map iteration order.
	// Include all mapping fields; this is an internal identity, not a wire format.
	//nolint:musttag // SourceTableMapping intentionally has no JSON serialization contract.
	identity, err := json.Marshal(struct {
		FlowName        string                              `json:"flow_name"`
		Tables          map[string]model.SourceTableMapping `json:"tables"`
		ExcludedOps     []operationType                     `json:"excluded_ops"`
		InternalVersion uint32                              `json:"internal_version"`
	}{req.FlowJobName, req.TableNameMapping, c.excludedOps, req.InternalVersion})
	return string(identity), err
}

func (c *MongoConnector) closeRetainedChangeStream() error {
	if c.changeStream == nil {
		return nil
	}
	stream := c.changeStream.stream
	c.changeStream = nil
	return stream.Close()
}
