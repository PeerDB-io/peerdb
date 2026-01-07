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
