package connclickhouse

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

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
