package utils

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

func RecordsToRawTableStream[Items model.Items](req *model.RecordsToStreamRequest[Items]) (*model.QRecordStream, error) {
	recordStream := model.NewQRecordStream(1 << 17)
	recordStream.SetSchema(qvalue.QRecordSchema{
		Fields: []qvalue.QField{
			{
				Name:     "_peerdb_uid",
				Type:     qvalue.QValueKindString,
				Nullable: false,
			},
			{
				Name:     "_peerdb_timestamp",
				Type:     qvalue.QValueKindInt64,
				Nullable: false,
			},
			{
				Name:     "_peerdb_destination_table_name",
				Type:     qvalue.QValueKindString,
				Nullable: false,
			},
			{
				Name:     "_peerdb_data",
				Type:     qvalue.QValueKindString,
				Nullable: false,
			},
			{
				Name:     "_peerdb_record_type",
				Type:     qvalue.QValueKindInt64,
				Nullable: true,
			},
			{
				Name:     "_peerdb_match_data",
				Type:     qvalue.QValueKindString,
				Nullable: true,
			},
			{
				Name:     "_peerdb_batch_id",
				Type:     qvalue.QValueKindInt64,
				Nullable: true,
			},
			{
				Name:     "_peerdb_unchanged_toast_columns",
				Type:     qvalue.QValueKindString,
				Nullable: true,
			},
		},
	})

	go func() {
		for record := range req.GetRecords() {
			record.PopulateCountMap(req.TableMapping)
			qRecord, err := recordToQRecordOrError(req.BatchID, record)
			if err != nil {
				recordStream.Close(err)
				return
			} else if qRecord != nil {
				recordStream.Records <- qRecord
			}
		}

		close(recordStream.Records)
	}()
	return recordStream, nil
}

func recordToQRecordOrError[Items model.Items](batchID int64, record model.Record[Items]) ([]qvalue.QValue, error) {
	var entries [8]qvalue.QValue
	switch typedRecord := record.(type) {
	case *model.InsertRecord[Items]:
		itemsJSON, err := model.ItemsToJSON(typedRecord.Items)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize insert record items to JSON: %w", err)
		}

		entries[3] = qvalue.QValueString{Val: itemsJSON}
		entries[4] = qvalue.QValueInt64{Val: 0}
		entries[5] = qvalue.QValueString{Val: ""}
		entries[7] = qvalue.QValueString{Val: ""}
	case *model.UpdateRecord[Items]:
		newItemsJSON, err := model.ItemsToJSON(typedRecord.NewItems)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize update record new items to JSON: %w", err)
		}
		oldItemsJSON, err := model.ItemsToJSON(typedRecord.OldItems)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize update record old items to JSON: %w", err)
		}

		entries[3] = qvalue.QValueString{Val: newItemsJSON}
		entries[4] = qvalue.QValueInt64{Val: 1}
		entries[5] = qvalue.QValueString{Val: oldItemsJSON}
		entries[7] = qvalue.QValueString{Val: KeysToString(typedRecord.UnchangedToastColumns)}

	case *model.DeleteRecord[Items]:
		itemsJSON, err := model.ItemsToJSON(typedRecord.Items)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize delete record items to JSON: %w", err)
		}

		entries[3] = qvalue.QValueString{Val: itemsJSON}
		entries[4] = qvalue.QValueInt64{Val: 2}
		entries[5] = qvalue.QValueString{Val: itemsJSON}
		entries[7] = qvalue.QValueString{Val: KeysToString(typedRecord.UnchangedToastColumns)}

	case *model.MessageRecord[Items]:
		return nil, nil

	default:
		return nil, fmt.Errorf("unknown record type: %T", typedRecord)
	}

	entries[0] = qvalue.QValueUUID{Val: uuid.New()}
	entries[1] = qvalue.QValueInt64{Val: time.Now().UnixNano()}
	entries[2] = qvalue.QValueString{Val: record.GetDestinationTableName()}
	entries[6] = qvalue.QValueInt64{Val: batchID}

	return entries[:], nil
}

func InitialiseTableRowsMap(tableMaps []*protos.TableMapping) map[string]*model.RecordTypeCounts {
	tableNameRowsMapping := make(map[string]*model.RecordTypeCounts, len(tableMaps))
	for _, mapping := range tableMaps {
		tableNameRowsMapping[mapping.DestinationTableIdentifier] = &model.RecordTypeCounts{}
	}

	return tableNameRowsMapping
}
