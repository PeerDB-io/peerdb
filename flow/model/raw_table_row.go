package model

import (
	"github.com/google/uuid"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// RawTableRow is the fixed CDC envelope, independent of source table schema.
// Although the Avro schema permits null in its last four fields, CDC always
// supplies values (including empty strings). Concrete fields select those same
// non-null union branches without allocating pointers or union maps.
//
//nolint:govet // Keep fields in Avro schema order for reviewability.
type RawTableRow struct {
	UID                   string `avro:"_peerdb_uid"`
	Timestamp             int64  `avro:"_peerdb_timestamp"`
	DestinationTableName  string `avro:"_peerdb_destination_table_name"`
	Data                  string `avro:"_peerdb_data"`
	RecordType            int64  `avro:"_peerdb_record_type"`
	MatchData             string `avro:"_peerdb_match_data"`
	BatchID               int64  `avro:"_peerdb_batch_id"`
	UnchangedToastColumns string `avro:"_peerdb_unchanged_toast_columns"`
}

// QRecord adapts the envelope for destinations using the generic row stream.
func (r *RawTableRow) QRecord() []types.QValue {
	return []types.QValue{
		types.QValueUUID{Val: uuid.MustParse(r.UID)},
		types.QValueInt64{Val: r.Timestamp},
		types.QValueString{Val: r.DestinationTableName},
		types.QValueString{Val: r.Data},
		types.QValueInt64{Val: r.RecordType},
		types.QValueString{Val: r.MatchData},
		types.QValueInt64{Val: r.BatchID},
		types.QValueString{Val: r.UnchangedToastColumns},
	}
}
