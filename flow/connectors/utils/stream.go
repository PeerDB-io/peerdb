package utils

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func RecordsToRawTableStream(
	req *model.RecordsToStreamRequest[model.RecordItems], numericTruncator model.StreamNumericTruncator,
) (*model.QRecordStream, error) {
	recordStream := model.NewQRecordStream(1024)
	recordStream.SetSchema(types.QRecordSchema{
		Fields: []types.QField{
			{
				Name:     "_peerdb_uid",
				Type:     types.QValueKindString,
				Nullable: false,
			},
			{
				Name:     "_peerdb_timestamp",
				Type:     types.QValueKindInt64,
				Nullable: false,
			},
			{
				Name:     "_peerdb_destination_table_name",
				Type:     types.QValueKindString,
				Nullable: false,
			},
			{
				Name:     "_peerdb_data",
				Type:     types.QValueKindString,
				Nullable: false,
			},
			{
				Name:     "_peerdb_record_type",
				Type:     types.QValueKindInt64,
				Nullable: true,
			},
			{
				Name:     "_peerdb_match_data",
				Type:     types.QValueKindString,
				Nullable: true,
			},
			{
				Name:     "_peerdb_batch_id",
				Type:     types.QValueKindInt64,
				Nullable: true,
			},
			{
				Name:     "_peerdb_unchanged_toast_columns",
				Type:     types.QValueKindString,
				Nullable: true,
			},
		},
	})

	go func() {
		for record := range req.GetRecords() {
			record.PopulateCountMap(req.TableMapping)
			qRecord, err := recordToQRecordOrError(
				req.BatchID, record, req.TargetDWH, req.UnboundedNumericAsString, numericTruncator,
			)
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

func recordToQRecordOrError(
	batchID int64, record model.Record[model.RecordItems], targetDWH protos.DBType, unboundedNumericAsString bool,
	numericTruncator model.StreamNumericTruncator,
) ([]types.QValue, error) {
	var entries [8]types.QValue
	switch typedRecord := record.(type) {
	case *model.InsertRecord[model.RecordItems]:
		tableNumericTruncator := numericTruncator.Get(typedRecord.DestinationTableName)
		preprocessedItems := truncateNumerics(
			typedRecord.Items, targetDWH, unboundedNumericAsString, tableNumericTruncator,
		)
		itemsJSON, err := model.ItemsToJSON(preprocessedItems)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize insert record items to JSON: %w", err)
		}

		entries[3] = types.QValueString{Val: itemsJSON}
		entries[4] = types.QValueInt64{Val: 0}
		entries[5] = types.QValueString{Val: ""}
		entries[7] = types.QValueString{Val: ""}
	case *model.UpdateRecord[model.RecordItems]:
		tableNumericTruncator := numericTruncator.Get(typedRecord.DestinationTableName)
		preprocessedItems := truncateNumerics(
			typedRecord.NewItems, targetDWH, unboundedNumericAsString, tableNumericTruncator,
		)
		newItemsJSON, err := model.ItemsToJSON(preprocessedItems)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize update record new items to JSON: %w", err)
		}
		oldItemsJSON, err := model.ItemsToJSON(typedRecord.OldItems)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize update record old items to JSON: %w", err)
		}

		entries[3] = types.QValueString{Val: newItemsJSON}
		entries[4] = types.QValueInt64{Val: 1}
		entries[5] = types.QValueString{Val: oldItemsJSON}
		entries[7] = types.QValueString{Val: KeysToString(typedRecord.UnchangedToastColumns)}

	case *model.DeleteRecord[model.RecordItems]:
		itemsJSON, err := model.ItemsToJSON(typedRecord.Items)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize delete record items to JSON: %w", err)
		}

		entries[3] = types.QValueString{Val: itemsJSON}
		entries[4] = types.QValueInt64{Val: 2}
		entries[5] = types.QValueString{Val: itemsJSON}
		entries[7] = types.QValueString{Val: KeysToString(typedRecord.UnchangedToastColumns)}

	case *model.MessageRecord[model.RecordItems]:
		return nil, nil

	default:
		return nil, fmt.Errorf("unknown record type: %T", typedRecord)
	}

	entries[0] = types.QValueUUID{Val: uuid.New()}
	entries[1] = types.QValueInt64{Val: time.Now().UnixNano()}
	entries[2] = types.QValueString{Val: record.GetDestinationTableName()}
	entries[6] = types.QValueInt64{Val: batchID}

	return entries[:], nil
}

func InitialiseTableRowsMap(tableMaps []*protos.TableMapping) map[string]*model.RecordTypeCounts {
	tableNameRowsMapping := make(map[string]*model.RecordTypeCounts, len(tableMaps))
	for _, mapping := range tableMaps {
		tableNameRowsMapping[mapping.DestinationTableIdentifier] = &model.RecordTypeCounts{}
	}

	return tableNameRowsMapping
}

func truncateNumerics(
	recordItems model.RecordItems, targetDWH protos.DBType, unboundedNumericAsString bool,
	numericTruncator model.CdcTableNumericTruncator,
) model.RecordItems {
	hasNumerics := false
	for col, val := range recordItems.ColToVal {
		if numericTruncator.Get(col).Stat != nil {
			if val.Kind() == types.QValueKindNumeric || val.Kind() == types.QValueKindArrayNumeric {
				hasNumerics = true
				break
			}
		}
	}
	if !hasNumerics {
		return recordItems
	}

	newItems := model.NewRecordItems(recordItems.Len())
	for col, val := range recordItems.ColToVal {
		newVal := val

		columnTruncator := numericTruncator.Get(col)
		if columnTruncator.Stat != nil {
			switch numeric := val.(type) {
			case types.QValueNumeric:
				destType := qvalue.GetNumericDestinationType(
					numeric.Precision, numeric.Scale, targetDWH, unboundedNumericAsString,
				)
				if destType.IsString {
					newVal = val
				} else {
					truncated, _, ok := qvalue.TruncateNumeric(
						numeric.Val, destType.Precision, destType.Scale, targetDWH, columnTruncator.Stat,
					)
					if !ok {
						truncated = decimal.Zero
					}
					newVal = types.QValueNumeric{
						Val:       truncated,
						Precision: destType.Precision,
						Scale:     destType.Scale,
					}
				}
			case types.QValueArrayNumeric:
				destType := qvalue.GetNumericDestinationType(
					numeric.Precision, numeric.Scale, targetDWH, unboundedNumericAsString,
				)
				if destType.IsString {
					newVal = val
				} else {
					truncatedArr := make([]decimal.Decimal, 0, len(numeric.Val))
					for _, num := range numeric.Val {
						truncated, _, ok := qvalue.TruncateNumeric(
							num, destType.Precision, destType.Scale, targetDWH, columnTruncator.Stat,
						)
						if !ok {
							truncated = decimal.Zero
						}
						truncatedArr = append(truncatedArr, truncated)
					}
					newVal = types.QValueArrayNumeric{
						Val:       truncatedArr,
						Precision: destType.Precision,
						Scale:     destType.Scale,
					}
				}
			}
		}
		newItems.ColToVal[col] = newVal
	}
	return newItems
}
