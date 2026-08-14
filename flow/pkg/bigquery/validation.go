package bigquery

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

// DatasetTable identifies a BigQuery table by dataset and table name.
type DatasetTable struct {
	Dataset string
	Table   string
}

// TablesHaveChangeHistoryEnabled checks the enable_change_history table option
// via INFORMATION_SCHEMA.TABLE_OPTIONS, one query per dataset for all tables in
// that dataset. This option is not exposed as a typed field on
// bigquery.TableMetadata.
func TablesHaveChangeHistoryEnabled(
	ctx context.Context, client *bigquery.Client, projectID string, tables []DatasetTable,
) (map[DatasetTable]bool, error) {
	tablesByDataset := make(map[string][]string)
	for _, dt := range tables {
		tablesByDataset[dt.Dataset] = append(tablesByDataset[dt.Dataset], dt.Table)
	}

	result := make(map[DatasetTable]bool, len(tables))
	for dataset, tableNames := range tablesByDataset {
		q := client.Query(fmt.Sprintf(
			"SELECT table_name, option_value FROM `%s`.INFORMATION_SCHEMA.TABLE_OPTIONS "+
				"WHERE table_name IN UNNEST(@table_names) AND option_name = 'enable_change_history'",
			dataset))
		q.Parameters = []bigquery.QueryParameter{{Name: "table_names", Value: tableNames}}
		q.DefaultProjectID = projectID
		q.DefaultDatasetID = dataset

		it, err := q.Read(ctx)
		if err != nil {
			return nil, err
		}
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if errors.Is(err, iterator.Done) {
					break
				}
				return nil, err
			}
			tableName, _ := row[0].(string)
			optionValue, _ := row[1].(string)
			result[DatasetTable{Dataset: dataset, Table: tableName}] = strings.EqualFold(optionValue, "true")
		}
	}
	return result, nil
}
