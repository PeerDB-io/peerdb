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

// TablesHaveChangeHistoryEnabled checks whether change history is enabled via
// INFORMATION_SCHEMA.TABLES.is_change_history_enabled, one query per dataset
// for all tables in that dataset.
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
			"SELECT table_name, is_change_history_enabled FROM `%s`.INFORMATION_SCHEMA.TABLES "+
				"WHERE table_name IN UNNEST(@table_names)",
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
			isEnabled, _ := row[1].(string)
			result[DatasetTable{Dataset: dataset, Table: tableName}] = strings.EqualFold(isEnabled, "YES")
		}
	}
	return result, nil
}
