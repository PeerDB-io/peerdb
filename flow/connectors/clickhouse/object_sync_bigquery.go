package connclickhouse

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

var objectSyncBigQueryExportFieldExpressionConverters = []fieldExpressionConverter{
	// RECORD: BigQuery cast RECORD to String
	func(ctx context.Context, config *insertFromTableFunctionConfig, sourceFieldIdentifier string, field types.QField) (string, error) {
		if field.OriginalType != string(bigquery.RecordFieldType) {
			return sourceFieldIdentifier, nil
		}

		switch field.Type {
		case types.QValueKindString:
			return fmt.Sprintf("CAST(%s, 'String')", sourceFieldIdentifier), nil
		case types.QValueKindArrayString:
			return fmt.Sprintf("arrayMap(x -> CAST(x, 'String'), %s)", sourceFieldIdentifier), nil
		default:
			return sourceFieldIdentifier, nil
		}
	},
}

// Converts below coverage quirks for BigQuery Avro exports:
// https://cloud.google.com/bigquery/docs/exporting-data#avro_export_details
var objectSyncBigQueryAvroExportFieldExpressionConverters = []fieldExpressionConverter{
	// DATE: BigQuery exports DATE as integer (days since epoch)
	func(ctx context.Context, config *insertFromTableFunctionConfig, sourceFieldIdentifier string, field types.QField) (string, error) {
		if field.Type != types.QValueKindDate {
			return sourceFieldIdentifier, nil
		}

		return fmt.Sprintf("toDate(%s)", sourceFieldIdentifier), nil
	},
	// ARRAY[DATE]: Arrays of DATE need element-wise conversion
	func(ctx context.Context, config *insertFromTableFunctionConfig, sourceFieldIdentifier string, field types.QField) (string, error) {
		if field.Type != types.QValueKindArrayDate {
			return sourceFieldIdentifier, nil
		}

		return fmt.Sprintf("arrayMap(x -> toDate(x), %s)", sourceFieldIdentifier), nil
	},
}
