package utils

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

func RecordsToRawTableStream(req *model.RecordsToStreamRequest) (*model.RecordsToStreamResponse, error) {
	recordStream := model.NewQRecordStream(1 << 17)
	err := recordStream.SetSchema(&model.QRecordSchema{
		Fields: []model.QField{
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
	if err != nil {
		return nil, err
	}

	go func() {
		for record := range req.GetRecords() {
			qRecordOrError := recordToQRecordOrError(req.TableMapping, req.BatchID, record)
			recordStream.Records <- qRecordOrError
		}

		close(recordStream.Records)
	}()

	return &model.RecordsToStreamResponse{
		Stream: recordStream,
	}, nil
}

func recordToQRecordOrError(tableMapping map[string]uint32, batchID int64, record model.Record) model.QRecordOrError {
	var entries [8]qvalue.QValue
	switch typedRecord := record.(type) {
	case *model.InsertRecord:
		// json.Marshal converts bytes in Hex automatically to BASE64 string.
		itemsJSON, err := typedRecord.Items.ToJSON()
		if err != nil {
			return model.QRecordOrError{
				Err: fmt.Errorf("failed to serialize insert record items to JSON: %w", err),
			}
		}

		entries[3] = qvalue.QValue{
			Kind:  qvalue.QValueKindString,
			Value: itemsJSON,
		}
		entries[4] = qvalue.QValue{
			Kind:  qvalue.QValueKindInt64,
			Value: 0,
		}
		entries[5] = qvalue.QValue{
			Kind:  qvalue.QValueKindString,
			Value: "",
		}
		entries[7] = qvalue.QValue{
			Kind:  qvalue.QValueKindString,
			Value: "",
		}
		tableMapping[typedRecord.DestinationTableName] += 1
	case *model.UpdateRecord:
		newItemsJSON, err := typedRecord.NewItems.ToJSON()
		if err != nil {
			return model.QRecordOrError{
				Err: fmt.Errorf("failed to serialize update record new items to JSON: %w", err),
			}
		}
		oldItemsJSON, err := typedRecord.OldItems.ToJSON()
		if err != nil {
			return model.QRecordOrError{
				Err: fmt.Errorf("failed to serialize update record old items to JSON: %w", err),
			}
		}

		entries[3] = qvalue.QValue{
			Kind:  qvalue.QValueKindString,
			Value: newItemsJSON,
		}
		entries[4] = qvalue.QValue{
			Kind:  qvalue.QValueKindInt64,
			Value: 1,
		}
		entries[5] = qvalue.QValue{
			Kind:  qvalue.QValueKindString,
			Value: oldItemsJSON,
		}
		entries[7] = qvalue.QValue{
			Kind:  qvalue.QValueKindString,
			Value: KeysToString(typedRecord.UnchangedToastColumns),
		}
		tableMapping[typedRecord.DestinationTableName] += 1
	case *model.DeleteRecord:
		itemsJSON, err := typedRecord.Items.ToJSON()
		if err != nil {
			return model.QRecordOrError{
				Err: fmt.Errorf("failed to serialize delete record items to JSON: %w", err),
			}
		}

		entries[3] = qvalue.QValue{
			Kind:  qvalue.QValueKindString,
			Value: itemsJSON,
		}
		entries[4] = qvalue.QValue{
			Kind:  qvalue.QValueKindInt64,
			Value: 2,
		}
		entries[5] = qvalue.QValue{
			Kind:  qvalue.QValueKindString,
			Value: itemsJSON,
		}
		entries[7] = qvalue.QValue{
			Kind:  qvalue.QValueKindString,
			Value: KeysToString(typedRecord.UnchangedToastColumns),
		}
		tableMapping[typedRecord.DestinationTableName] += 1
	default:
		return model.QRecordOrError{
			Err: fmt.Errorf("unknown record type: %T", typedRecord),
		}
	}

	entries[0] = qvalue.QValue{
		Kind:  qvalue.QValueKindString,
		Value: uuid.New().String(),
	}
	entries[1] = qvalue.QValue{
		Kind:  qvalue.QValueKindInt64,
		Value: time.Now().UnixNano(),
	}
	entries[2] = qvalue.QValue{
		Kind:  qvalue.QValueKindString,
		Value: record.GetDestinationTableName(),
	}
	entries[6] = qvalue.QValue{
		Kind:  qvalue.QValueKindInt64,
		Value: batchID,
	}

	return model.QRecordOrError{
		Record: entries[:],
	}
}
