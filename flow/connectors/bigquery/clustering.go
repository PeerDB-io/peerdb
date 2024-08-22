package connbigquery

import (
	"cloud.google.com/go/bigquery"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

// Columns in BigQuery which are supported for clustering of tables
// Reference: https://cloud.google.com/bigquery/docs/clustered-tables#cluster_column_types
var supportedClusteringTypes = map[bigquery.FieldType]struct{}{
	bigquery.StringFieldType:     {},
	bigquery.IntegerFieldType:    {},
	bigquery.BooleanFieldType:    {},
	bigquery.TimestampFieldType:  {},
	bigquery.DateFieldType:       {},
	bigquery.DateTimeFieldType:   {},
	bigquery.NumericFieldType:    {},
	bigquery.BigNumericFieldType: {},
	bigquery.GeographyFieldType:  {},
}

func isSupportedClusteringType(fieldType bigquery.FieldType) bool {
	_, ok := supportedClusteringTypes[fieldType]
	return ok
}

func obtainClusteringColumns(tableSchema *protos.TableSchema) []string {
	numPkeyCols := len(tableSchema.PrimaryKeyColumns)
	supportedPkeyColsForClustering := make([]string, 0, numPkeyCols)
	isColPrimary := make(map[string]bool, numPkeyCols)
	for _, pkeyCol := range tableSchema.PrimaryKeyColumns {
		isColPrimary[pkeyCol] = true
	}
	for _, col := range tableSchema.Columns {
		if _, ok := isColPrimary[col.Name]; ok {
			if bigqueryType := qValueKindToBigQueryType(col, tableSchema.NullableEnabled); ok {
				if isSupportedClusteringType(bigqueryType.Type) {
					supportedPkeyColsForClustering = append(supportedPkeyColsForClustering, col.Name)
				}
			}
		}
	}
	return supportedPkeyColsForClustering
}
