package utils

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func RecordsToRawTableStream(
	req *model.RecordsToStreamRequest[model.RecordItems], numericTruncator model.StreamNumericTruncator,
) (*model.QRecordStream, error) {
	return recordsToRawTableStream(req, numericTruncator, func(row *model.RawTableRow) []types.QValue {
		return row.QRecord()
	}), nil
}

// RecordsToRawTableAvroStream retains the fixed CDC envelope through Avro encoding.
func RecordsToRawTableAvroStream(
	req *model.RecordsToStreamRequest[model.RecordItems], numericTruncator model.StreamNumericTruncator,
) (*model.RecordStream[*model.RawTableRow], error) {
	return recordsToRawTableStream(req, numericTruncator, func(row *model.RawTableRow) *model.RawTableRow { return row }), nil
}

func recordsToRawTableStream[T any](
	req *model.RecordsToStreamRequest[model.RecordItems], numericTruncator model.StreamNumericTruncator,
	convert func(*model.RawTableRow) T,
) *model.RecordStream[T] {
	recordStream := model.NewRecordStream[T](1024)
	recordStream.SetSchema(rawTableSchema())

	go func() {
		for record := range req.GetRecords() {
			record.PopulateCountMap(req.TableMapping)
			qRecord, err := recordToRawTableRow(
				req.BatchID, record, req.TargetDWH, req.UnboundedNumericAsString, numericTruncator,
			)
			if err != nil {
				recordStream.Close(err)
				return
			} else if qRecord != nil {
				recordStream.Records <- convert(qRecord)
			}
		}

		recordStream.Close(nil)
	}()
	return recordStream
}

func recordToRawTableRow(
	batchID int64, record model.Record[model.RecordItems], targetDWH protos.DBType, unboundedNumericAsString bool,
	numericTruncator model.StreamNumericTruncator,
) (*model.RawTableRow, error) {
	row := &model.RawTableRow{}
	jsonOpts := rawTableJSONOptions(targetDWH)
	switch typedRecord := record.(type) {
	case *model.InsertRecord[model.RecordItems]:
		tableNumericTruncator := numericTruncator.Get(typedRecord.DestinationTableName)
		preprocessedItems := truncateNumerics(
			typedRecord.Items, targetDWH, unboundedNumericAsString, tableNumericTruncator,
		)
		itemsJSON, err := preprocessedItems.ToJSONWithOptions(jsonOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize insert record items to JSON: %w", err)
		}

		row.Data = itemsJSON
		row.RecordType = 0
		row.MatchData = ""
		row.UnchangedToastColumns = ""
	case *model.UpdateRecord[model.RecordItems]:
		tableNumericTruncator := numericTruncator.Get(typedRecord.DestinationTableName)
		preprocessedItems := truncateNumerics(
			typedRecord.NewItems, targetDWH, unboundedNumericAsString, tableNumericTruncator,
		)
		newItemsJSON, err := preprocessedItems.ToJSONWithOptions(jsonOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize update record new items to JSON: %w", err)
		}
		oldItemsJSON, err := typedRecord.OldItems.ToJSONWithOptions(jsonOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize update record old items to JSON: %w", err)
		}

		row.Data = newItemsJSON
		row.RecordType = 1
		row.MatchData = oldItemsJSON
		row.UnchangedToastColumns = KeysToString(typedRecord.UnchangedToastColumns)

	case *model.DeleteRecord[model.RecordItems]:
		itemsJSON, err := typedRecord.Items.ToJSONWithOptions(jsonOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize delete record items to JSON: %w", err)
		}

		row.Data = itemsJSON
		row.RecordType = 2
		row.MatchData = itemsJSON
		row.UnchangedToastColumns = KeysToString(typedRecord.UnchangedToastColumns)

	case *model.MessageRecord[model.RecordItems]:
		return nil, nil

	default:
		return nil, fmt.Errorf("unknown record type: %T", typedRecord)
	}

	row.UID = uuid.NewString()
	row.Timestamp = time.Now().UnixNano()
	row.DestinationTableName = record.GetDestinationTableName()
	row.BatchID = batchID

	return row, nil
}

// RecordsToTypedCDCStream converts a single table's CDC record channel directly
// into a typed QRecordStream, for destinations (currently ClickHouse) that can
// INSERT the result straight into their final table without a JSON-blob raw
// table in between.
func RecordsToTypedCDCStream(
	records <-chan model.Record[model.RecordItems],
	destinationTableName string,
	schema types.QRecordSchema,
	businessFields []types.QField,
	sourceColumnByDest map[string]string,
	targetDWH protos.DBType,
	unboundedNumericAsString bool,
	numericTruncator model.StreamNumericTruncator,
	rowCounts *model.RecordTypeCounts,
) (*model.QRecordStream, error) {
	countMap := map[string]*model.RecordTypeCounts{destinationTableName: rowCounts}

	recordStream := model.NewQRecordStream(1024)
	recordStream.SetSchema(schema)

	go func() {
		tableNumericTruncator := numericTruncator.Get(destinationTableName)
		for record := range records {
			row, err := typedCDCRow(schema, businessFields, sourceColumnByDest, record,
				targetDWH, unboundedNumericAsString, tableNumericTruncator)
			if err != nil {
				recordStream.Close(err)
				return
			}
			if rowCounts != nil {
				record.PopulateCountMap(countMap)
			}
			if row != nil {
				recordStream.Records <- row
			}
		}
		close(recordStream.Records)
	}()
	return recordStream, nil
}

func typedCDCRow(
	schema types.QRecordSchema,
	businessFields []types.QField,
	sourceColumnByDest map[string]string,
	record model.Record[model.RecordItems],
	targetDWH protos.DBType,
	unboundedNumericAsString bool,
	tableNumericTruncator model.CdcTableNumericTruncator,
) ([]types.QValue, error) {
	var items model.RecordItems
	var isDeleted int64
	switch typedRecord := record.(type) {
	case *model.InsertRecord[model.RecordItems]:
		items = typedRecord.Items
	case *model.UpdateRecord[model.RecordItems]:
		items = typedRecord.NewItems
	case *model.DeleteRecord[model.RecordItems]:
		items = typedRecord.Items
		isDeleted = 1
	default:
		return nil, fmt.Errorf("unknown record type: %T", typedRecord)
	}

	items = truncateNumerics(items, targetDWH, unboundedNumericAsString, tableNumericTruncator)

	row := make([]types.QValue, 0, len(schema.Fields))
	for _, field := range businessFields {
		val := items.GetColumnValue(sourceColumnByDest[field.Name])
		if val == nil {
			// column was dropped from the source
			// use Null for nullable columns, and a default value for non-nullable columns
			// otherwise the destination will reject the row
			if field.Nullable {
				val = types.QValueNull(field.Type)
			} else {
				val = field.Type.DefaultValue()
			}
		}
		row = append(row, val)
	}
	row = append(row,
		types.QValueInt64{Val: isDeleted},
		types.QValueInt64{Val: record.GetCommitTime().UnixNano()},
	)
	if len(row) != len(schema.Fields) {
		return nil, fmt.Errorf("typed CDC row has %d values for a %d-field schema", len(row), len(schema.Fields))
	}
	return row, nil
}

func rawTableJSONOptions(target protos.DBType) model.ToJSONOptions {
	opts := model.NewToJSONOptions(nil, true)
	if target == protos.DBType_SNOWFLAKE {
		opts.ClearValuesOverBytes = shared.SnowflakeClearValueThresholdBytes
	}
	return opts
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

func rawTableSchema() types.QRecordSchema {
	return types.QRecordSchema{
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
	}
}
