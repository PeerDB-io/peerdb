package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	peer_bq "github.com/PeerDB-io/peer-flow/connectors/bigquery"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"google.golang.org/api/iterator"
)

type BigQueryTestHelper struct {
	// runID uniquely identifies the test run to namespace stateful schemas.
	runID int64
	// config is the BigQuery config.
	Config *protos.BigqueryConfig
	// client to talk to BigQuery
	client *bigquery.Client
	// dataset to use for testing.
	datasetName string
}

// NewBigQueryTestHelper creates a new BigQueryTestHelper.
func NewBigQueryTestHelper() (*BigQueryTestHelper, error) {
	// random 64 bit int to namespace stateful schemas.
	runID := rand.Int63()

	jsonPath := os.Getenv("TEST_BQ_CREDS")
	if jsonPath == "" {
		return nil, fmt.Errorf("TEST_BQ_CREDS env var not set")
	}

	content, err := readFileToBytes(jsonPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var config protos.BigqueryConfig
	err = json.Unmarshal(content, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal json: %w", err)
	}

	// suffix the dataset with the runID to namespace stateful schemas.
	config.DatasetId = fmt.Sprintf("%s_%d", config.DatasetId, runID)

	bqsa, err := peer_bq.NewBigQueryServiceAccount(&config)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQueryServiceAccount: %v", err)
	}

	client, err := bqsa.CreateBigQueryClient(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to create helper BigQuery client: %v", err)
	}

	return &BigQueryTestHelper{
		runID:       runID,
		Config:      &config,
		client:      client,
		datasetName: config.DatasetId,
	}, nil
}

// datasetExists checks if the dataset exists.
func (b *BigQueryTestHelper) datasetExists() (bool, error) {
	dataset := b.client.Dataset(b.Config.DatasetId)
	meta, err := dataset.Metadata(context.Background())
	if err != nil {
		// if err message contains `notFound` then dataset does not exist.
		// first we cast the error to a bigquery.Error
		if strings.Contains(err.Error(), "notFound") {
			fmt.Printf("dataset %s does not exist\n", b.Config.DatasetId)
			return false, nil
		}

		return false, fmt.Errorf("failed to get dataset metadata: %w", err)
	}

	if meta == nil {
		return false, nil
	}

	return true, nil
}

// RecreateDataset recreates the dataset, i.e, deletes it if exists and creates it again.
func (b *BigQueryTestHelper) RecreateDataset() error {
	exists, err := b.datasetExists()
	if err != nil {
		return fmt.Errorf("failed to check if dataset %s exists: %w", b.Config.DatasetId, err)
	}

	dataset := b.client.Dataset(b.Config.DatasetId)
	if exists {
		err := dataset.DeleteWithContents(context.Background())
		if err != nil {
			return fmt.Errorf("failed to delete dataset: %w", err)
		}
	}

	err = dataset.Create(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	fmt.Printf("created dataset %s successfully\n", b.Config.DatasetId)
	return nil
}

// DropDataset drops the dataset.
func (b *BigQueryTestHelper) DropDataset() error {
	exists, err := b.datasetExists()
	if err != nil {
		return fmt.Errorf("failed to check if dataset %s exists: %w", b.Config.DatasetId, err)
	}

	if !exists {
		return nil
	}

	dataset := b.client.Dataset(b.Config.DatasetId)
	err = dataset.DeleteWithContents(context.Background())
	if err != nil {
		return fmt.Errorf("failed to delete dataset: %w", err)
	}

	return nil
}

// RunCommand runs the given command.
func (b *BigQueryTestHelper) RunCommand(command string) error {
	_, err := b.client.Query(command).Read(context.Background())
	if err != nil {
		return fmt.Errorf("failed to run command: %w", err)
	}

	return nil
}

// CountRows(tableName) returns the number of rows in the given table.
func (b *BigQueryTestHelper) CountRows(tableName string) (int, error) {
	command := fmt.Sprintf("SELECT COUNT(*) FROM `%s.%s`", b.Config.DatasetId, tableName)
	it, err := b.client.Query(command).Read(context.Background())
	if err != nil {
		return 0, fmt.Errorf("failed to run command: %w", err)
	}

	var row []bigquery.Value
	for {
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("failed to iterate over query results: %w", err)
		}
	}

	cntI64, ok := row[0].(int64)
	if !ok {
		return 0, fmt.Errorf("failed to convert row count to int64")
	}

	return int(cntI64), nil
}

func toQValue(bqValue bigquery.Value) (model.QValue, error) {
	// Based on the real type of the bigquery.Value, we create a model.QValue
	switch v := bqValue.(type) {
	case int, int32:
		return model.QValue{Kind: model.QValueKindInt32, Value: v}, nil
	case int64:
		return model.QValue{Kind: model.QValueKindInt64, Value: v}, nil
	case float32:
		return model.QValue{Kind: model.QValueKindFloat32, Value: v}, nil
	case float64:
		return model.QValue{Kind: model.QValueKindFloat64, Value: v}, nil
	case string:
		return model.QValue{Kind: model.QValueKindString, Value: v}, nil
	case bool:
		return model.QValue{Kind: model.QValueKindBoolean, Value: v}, nil
	case time.Time:
		val, err := model.NewExtendedTime(v, model.DateTimeKindType, "")
		if err != nil {
			return model.QValue{}, fmt.Errorf("failed to create ExtendedTime: %w", err)
		}
		return model.QValue{
			Kind:  model.QValueKindETime,
			Value: val,
		}, nil
	case *big.Rat:
		return model.QValue{Kind: model.QValueKindNumeric, Value: v}, nil
	case []uint8:
		return model.QValue{Kind: model.QValueKindBytes, Value: v}, nil
	default:
		// If type is unsupported, return error
		return model.QValue{}, fmt.Errorf("bqHelper unsupported type %T", v)
	}
}

func (b *BigQueryTestHelper) ExecuteAndProcessQuery(query string) (*model.QRecordBatch, error) {
	it, err := b.client.Query(query).Read(context.Background())
	if err != nil {
		fmt.Printf("failed to run command: %v\n", err)
		return nil, fmt.Errorf("failed to run command: %w", err)
	}

	var records []*model.QRecord
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			fmt.Printf("failed to iterate over query results: %v\n", err)
			return nil, fmt.Errorf("failed to iterate over query results: %w", err)
		}

		// Convert []bigquery.Value to []model.QValue
		qValues := make([]model.QValue, len(row))
		for i, val := range row {
			qv, err := toQValue(val)
			if err != nil {
				return nil, err
			}
			qValues[i] = qv
		}

		// Create a QRecord
		record := model.NewQRecord(len(qValues))
		for i, qv := range qValues {
			record.Set(i, qv)
		}

		records = append(records, record)
	}

	// Now you should fill the column names as well. Here we assume the schema is
	// retrieved from the query itself
	var columnNames []string
	if it.Schema != nil {
		columnNames = make([]string, len(it.Schema))
		for i, fieldSchema := range it.Schema {
			columnNames[i] = fieldSchema.Name
		}
	}

	// Return a QRecordBatch
	return &model.QRecordBatch{
		NumRecords:  uint32(len(records)),
		Records:     records,
		ColumnNames: columnNames,
	}, nil
}
