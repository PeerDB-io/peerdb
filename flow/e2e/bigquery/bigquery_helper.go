package e2e_bigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	peer_bq "github.com/PeerDB-io/peer-flow/connectors/bigquery"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
	"google.golang.org/api/iterator"
)

type BigQueryTestHelper struct {
	// runID uniquely identifies the test run to namespace stateful schemas.
	runID uint64
	// config is the BigQuery config.
	Config *protos.BigqueryConfig
	// peer struct holder BigQuery
	Peer *protos.Peer
	// client to talk to BigQuery
	client *bigquery.Client
	// dataset to use for testing.
	datasetName string
}

// NewBigQueryTestHelper creates a new BigQueryTestHelper.
func NewBigQueryTestHelper() (*BigQueryTestHelper, error) {
	// random 64 bit int to namespace stateful schemas.
	runID, err := shared.RandomUInt64()
	if err != nil {
		return nil, fmt.Errorf("failed to generate random uint64: %w", err)
	}

	jsonPath := os.Getenv("TEST_BQ_CREDS")
	if jsonPath == "" {
		return nil, fmt.Errorf("TEST_BQ_CREDS env var not set")
	}

	content, err := e2e.ReadFileToBytes(jsonPath)
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

	peer := generateBQPeer(&config)

	return &BigQueryTestHelper{
		runID:       runID,
		Config:      &config,
		client:      client,
		datasetName: config.DatasetId,
		Peer:        peer,
	}, nil
}

func generateBQPeer(bigQueryConfig *protos.BigqueryConfig) *protos.Peer {
	ret := &protos.Peer{}
	ret.Name = "test_bq_peer"
	ret.Type = protos.DBType_BIGQUERY

	ret.Config = &protos.Peer_BigqueryConfig{
		BigqueryConfig: bigQueryConfig,
	}

	return ret
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

// countRows(tableName) returns the number of rows in the given table.
func (b *BigQueryTestHelper) countRows(tableName string) (int, error) {
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

func toQValue(bqValue bigquery.Value) (qvalue.QValue, error) {
	// Based on the real type of the bigquery.Value, we create a qvalue.QValue
	switch v := bqValue.(type) {
	case int, int32:
		return qvalue.QValue{Kind: qvalue.QValueKindInt32, Value: v}, nil
	case int64:
		return qvalue.QValue{Kind: qvalue.QValueKindInt64, Value: v}, nil
	case float32:
		return qvalue.QValue{Kind: qvalue.QValueKindFloat32, Value: v}, nil
	case float64:
		return qvalue.QValue{Kind: qvalue.QValueKindFloat64, Value: v}, nil
	case string:
		return qvalue.QValue{Kind: qvalue.QValueKindString, Value: v}, nil
	case bool:
		return qvalue.QValue{Kind: qvalue.QValueKindBoolean, Value: v}, nil
	case civil.Date:
		return qvalue.QValue{Kind: qvalue.QValueKindDate, Value: bqValue.(civil.Date).In(time.UTC)}, nil
	case time.Time:
		return qvalue.QValue{Kind: qvalue.QValueKindTimestamp, Value: v}, nil
	case *big.Rat:
		return qvalue.QValue{Kind: qvalue.QValueKindNumeric, Value: v}, nil
	case []uint8:
		return qvalue.QValue{Kind: qvalue.QValueKindBytes, Value: v}, nil
	case []bigquery.Value:
		// If the type is an array, we need to convert each element
		// we can assume all elements are of the same type, let us use first element
		if len(v) == 0 {
			return qvalue.QValue{Kind: qvalue.QValueKindInvalid, Value: nil}, nil
		}

		firstElement := v[0]
		switch firstElement.(type) {
		case int, int32:
			var arr []int32
			for _, val := range v {
				arr = append(arr, val.(int32))
			}
			return qvalue.QValue{Kind: qvalue.QValueKindArrayInt32, Value: arr}, nil
		case int64:
			var arr []int64
			for _, val := range v {
				arr = append(arr, val.(int64))
			}
			return qvalue.QValue{Kind: qvalue.QValueKindArrayInt64, Value: arr}, nil
		case float32:
			var arr []float32
			for _, val := range v {
				arr = append(arr, val.(float32))
			}
			return qvalue.QValue{Kind: qvalue.QValueKindArrayFloat32, Value: arr}, nil
		case float64:
			var arr []float64
			for _, val := range v {
				arr = append(arr, val.(float64))
			}
			return qvalue.QValue{Kind: qvalue.QValueKindArrayFloat64, Value: arr}, nil
		case string:
			var arr []string
			for _, val := range v {
				arr = append(arr, val.(string))
			}
			return qvalue.QValue{Kind: qvalue.QValueKindArrayString, Value: arr}, nil
		}

		// If type is unsupported, return error
		return qvalue.QValue{}, fmt.Errorf("bqHelper unsupported type %T", v)
	case nil:
		return qvalue.QValue{Kind: qvalue.QValueKindInvalid, Value: nil}, nil
	default:
		// If type is unsupported, return error
		return qvalue.QValue{}, fmt.Errorf("bqHelper unsupported type %T", v)
	}
}

func bqFieldSchemaToQField(fieldSchema *bigquery.FieldSchema) (model.QField, error) {
	qValueKind, err := peer_bq.BigQueryTypeToQValueKind(fieldSchema.Type)
	if err != nil {
		return model.QField{}, err
	}

	return model.QField{
		Name:     fieldSchema.Name,
		Type:     qValueKind,
		Nullable: !fieldSchema.Required,
	}, nil
}

// bqSchemaToQRecordSchema converts a bigquery schema to a QRecordSchema.
func bqSchemaToQRecordSchema(schema bigquery.Schema) (*model.QRecordSchema, error) {
	fields := make([]model.QField, 0, len(schema))
	for _, fieldSchema := range schema {
		qField, err := bqFieldSchemaToQField(fieldSchema)
		if err != nil {
			return nil, err
		}
		fields = append(fields, qField)
	}

	return &model.QRecordSchema{
		Fields: fields,
	}, nil
}

func (b *BigQueryTestHelper) ExecuteAndProcessQuery(query string) (*model.QRecordBatch, error) {
	it, err := b.client.Query(query).Read(context.Background())
	if err != nil {
		fmt.Printf("failed to run command: %v\n", err)
		return nil, fmt.Errorf("failed to run command: %w", err)
	}

	var records []model.QRecord
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

		// Convert []bigquery.Value to []qvalue.QValue
		qValues := make([]qvalue.QValue, len(row))
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
	var schema *model.QRecordSchema
	if it.Schema != nil {
		schema, err = bqSchemaToQRecordSchema(it.Schema)
		if err != nil {
			return nil, err
		}
	}

	// Return a QRecordBatch
	return &model.QRecordBatch{
		NumRecords: uint32(len(records)),
		Records:    records,
		Schema:     schema,
	}, nil
}

/*
if the function errors or there are nulls, the function returns false
else true
*/
func (b *BigQueryTestHelper) CheckNull(tableName string, ColName []string) (bool, error) {
	if len(ColName) == 0 {
		return true, nil
	}
	joinedString := strings.Join(ColName, " is null or ") + " is null"
	command := fmt.Sprintf("SELECT COUNT(*) FROM `%s.%s` WHERE %s",
		b.Config.DatasetId, tableName, joinedString)
	it, err := b.client.Query(command).Read(context.Background())
	if err != nil {
		return false, fmt.Errorf("failed to run command: %w", err)
	}

	var row []bigquery.Value
	for {
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return false, fmt.Errorf("failed to iterate over query results: %w", err)
		}
	}

	cntI64, ok := row[0].(int64)
	if !ok {
		return false, fmt.Errorf("failed to convert row count to int64")
	}
	if cntI64 > 0 {
		return false, nil
	} else {
		return true, nil
	}
}

func qValueKindToBqColTypeString(val qvalue.QValueKind) (string, error) {
	switch val {
	case qvalue.QValueKindInt16:
		return "INT64", nil
	case qvalue.QValueKindInt32:
		return "INT64", nil
	case qvalue.QValueKindInt64:
		return "INT64", nil
	case qvalue.QValueKindFloat32:
		return "FLOAT64", nil
	case qvalue.QValueKindFloat64:
		return "FLOAT64", nil
	case qvalue.QValueKindString:
		return "STRING", nil
	case qvalue.QValueKindBoolean:
		return "BOOL", nil
	case qvalue.QValueKindTimestamp:
		return "TIMESTAMP", nil
	case qvalue.QValueKindBytes, qvalue.QValueKindBit:
		return "BYTES", nil
	case qvalue.QValueKindNumeric:
		return "NUMERIC", nil
	case qvalue.QValueKindArrayString:
		return "ARRAY<STRING>", nil
	case qvalue.QValueKindArrayInt32:
		return "ARRAY<INT64>", nil
	case qvalue.QValueKindArrayInt64:
		return "ARRAY<INT64>", nil
	case qvalue.QValueKindArrayFloat32:
		return "ARRAY<FLOAT64>", nil
	case qvalue.QValueKindArrayFloat64:
		return "ARRAY<FLOAT64>", nil
	case qvalue.QValueKindJSON:
		return "STRING", nil
	default:
		return "", fmt.Errorf("[bq] unsupported QValueKind: %v", val)
	}
}

func (b *BigQueryTestHelper) CreateTable(tableName string, schema *model.QRecordSchema) error {
	fields := make([]string, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		bqType, err := qValueKindToBqColTypeString(field.Type)
		if err != nil {
			return err
		}
		fields = append(fields, fmt.Sprintf("`%s` %s", field.Name, bqType))
	}

	command := fmt.Sprintf("CREATE TABLE %s.%s (%s)", b.datasetName, tableName, strings.Join(fields, ", "))
	fmt.Printf("creating table %s with command %s\n", tableName, command)

	err := b.RunCommand(command)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}
