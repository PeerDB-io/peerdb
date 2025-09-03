package shared

import "strings"

func SplitDatasetTable(identifier string) (string, string) {
	datasetID, tableName, _ := strings.Cut(identifier, ".")
	return datasetID, tableName
}

func SplitDatasetTableWithDefault(identifier, defaultDataset string) (string, string) {
	datasetID, tableName := SplitDatasetTable(identifier)
	if tableName == "" {
		return defaultDataset, datasetID
	}
	return datasetID, tableName
}
