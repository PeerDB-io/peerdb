package e2e_bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/shopspring/decimal"
	"google.golang.org/api/iterator"

	peer_bq "github.com/PeerDB-io/peer-flow/connectors/bigquery"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

type BigQueryTestHelper struct {
	// config is the BigQuery config.
	Config *protos.BigqueryConfig
	// client to talk to BigQuery
	client *bigquery.Client
	// runID uniquely identifies the test run to namespace stateful schemas.
	runID uint64
}

// NewBigQueryTestHelper creates a new BigQueryTestHelper.
func NewBigQueryTestHelper(t *testing.T) (*BigQueryTestHelper, error) {
	t.Helper()
	// random 64 bit int to namespace stateful schemas.
	//nolint:gosec // number has no cryptographic significance
	runID := rand.Uint64()

	jsonPath := os.Getenv("TEST_BQ_CREDS")
	if jsonPath == "" {
		return nil, errors.New("TEST_BQ_CREDS env var not set")
	}

	content, err := e2eshared.ReadFileToBytes(jsonPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var config *protos.BigqueryConfig
	if err := json.Unmarshal(content, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal json: %w", err)
	}

	// suffix the dataset with the runID to namespace stateful schemas.
	config.DatasetId = fmt.Sprintf("%s_%d", config.DatasetId, runID)

	bqsa, err := peer_bq.NewBigQueryServiceAccount(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQueryServiceAccount: %v", err)
	}

	client, err := bqsa.CreateBigQueryClient(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to create helper BigQuery client: %v", err)
	}

	return &BigQueryTestHelper{
		runID:  runID,
		Config: config,
		client: client,
	}, nil
}

// datasetExists checks if the dataset exists.
func (b *BigQueryTestHelper) datasetExists(dataset *bigquery.Dataset) (bool, error) {
	meta, err := dataset.Metadata(context.Background())
	if err != nil {
		// if err message contains `notFound` then dataset does not exist.
		if strings.Contains(err.Error(), "notFound") {
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
	dataset := b.client.Dataset(b.Config.DatasetId)

	exists, err := b.datasetExists(dataset)
	if err != nil {
		return fmt.Errorf("failed to check if dataset %s exists: %w", b.Config.DatasetId, err)
	}

	if exists {
		err := dataset.DeleteWithContents(context.Background())
		if err != nil {
			return fmt.Errorf("failed to delete dataset: %w", err)
		}
	}

	err = dataset.Create(context.Background(), &bigquery.DatasetMetadata{
		DefaultTableExpiration:     time.Hour,
		DefaultPartitionExpiration: time.Hour,
	})
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	return nil
}

// DropDataset drops the dataset.
func (b *BigQueryTestHelper) DropDataset(datasetName string) error {
	dataset := b.client.Dataset(datasetName)
	exists, err := b.datasetExists(dataset)
	if err != nil {
		return fmt.Errorf("failed to check if dataset %s exists: %w", b.Config.DatasetId, err)
	}

	if exists {
		err = dataset.DeleteWithContents(context.Background())
		if err != nil {
			return fmt.Errorf("failed to delete dataset: %w", err)
		}
	}

	return nil
}

// RunCommand runs the given command.
func (b *BigQueryTestHelper) RunCommand(command string) error {
	q := b.client.Query(command)
	q.DisableQueryCache = true
	_, err := q.Read(context.Background())
	if err != nil {
		return fmt.Errorf("failed to run command: %w", err)
	}

	return nil
}

// countRows(tableName) returns the number of rows in the given table.
func (b *BigQueryTestHelper) countRows(tableName string) (int, error) {
	return b.countRowsWithDataset(b.Config.DatasetId, tableName, "")
}

func (b *BigQueryTestHelper) countRowsWithDataset(dataset, tableName string, nonNullCol string) (int, error) {
	command := fmt.Sprintf("SELECT COUNT(*) FROM `%s.%s`", dataset, tableName)
	if nonNullCol != "" {
		command = "SELECT COUNT(CASE WHEN " + nonNullCol +
			" IS NOT NULL THEN 1 END) AS non_null_count FROM `" + dataset + "." + tableName + "`;"
	}
	q := b.client.Query(command)
	q.DisableQueryCache = true
	it, err := q.Read(context.Background())
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
		return 0, errors.New("failed to convert row count to int64")
	}

	return int(cntI64), nil
}

func toQValue(bqValue bigquery.Value) (qvalue.QValue, error) {
	// Based on the real type of the bigquery.Value, we create a qvalue.QValue
	switch v := bqValue.(type) {
	case int:
		return qvalue.QValueInt32{Val: int32(v)}, nil
	case int32:
		return qvalue.QValueInt32{Val: v}, nil
	case int64:
		return qvalue.QValueInt64{Val: v}, nil
	case float32:
		return qvalue.QValueFloat32{Val: v}, nil
	case float64:
		return qvalue.QValueFloat64{Val: v}, nil
	case string:
		return qvalue.QValueString{Val: v}, nil
	case bool:
		return qvalue.QValueBoolean{Val: v}, nil
	case civil.Date:
		return qvalue.QValueDate{Val: v.In(time.UTC)}, nil
	case civil.Time:
		tm := time.Unix(int64(v.Hour)*3600+int64(v.Minute)*60+int64(v.Second), int64(v.Nanosecond))
		return qvalue.QValueTime{Val: tm}, nil
	case time.Time:
		return qvalue.QValueTimestamp{Val: v}, nil
	case *big.Rat:
		return qvalue.QValueNumeric{Val: decimal.NewFromBigRat(v, 32)}, nil
	case []uint8:
		return qvalue.QValueBytes{Val: v}, nil
	case []bigquery.Value:
		// If the type is an array, we need to convert each element
		// we can assume all elements are of the same type, let us use first element
		if len(v) == 0 {
			return qvalue.QValueNull(qvalue.QValueKindInvalid), nil
		}

		firstElement := v[0]
		switch et := firstElement.(type) {
		case int, int32:
			var arr []int32
			for _, val := range v {
				arr = append(arr, val.(int32))
			}
			return qvalue.QValueArrayInt32{Val: arr}, nil
		case int64:
			var arr []int64
			for _, val := range v {
				arr = append(arr, val.(int64))
			}
			return qvalue.QValueArrayInt64{Val: arr}, nil
		case float32:
			var arr []float32
			for _, val := range v {
				arr = append(arr, val.(float32))
			}
			return qvalue.QValueArrayFloat32{Val: arr}, nil
		case float64:
			var arr []float64
			for _, val := range v {
				arr = append(arr, val.(float64))
			}
			return qvalue.QValueArrayFloat64{Val: arr}, nil
		case string:
			var arr []string
			for _, val := range v {
				arr = append(arr, val.(string))
			}
			return qvalue.QValueArrayString{Val: arr}, nil
		case time.Time:
			var arr []time.Time
			for _, val := range v {
				arr = append(arr, val.(time.Time))
			}
			return qvalue.QValueArrayTimestamp{Val: arr}, nil
		case civil.Date:
			var arr []time.Time
			for _, val := range v {
				arr = append(arr, val.(civil.Date).In(time.UTC))
			}
			return qvalue.QValueArrayDate{Val: arr}, nil
		case bool:
			var arr []bool
			for _, val := range v {
				arr = append(arr, val.(bool))
			}
			return qvalue.QValueArrayBoolean{Val: arr}, nil
		default:
			// If type is unsupported, return error
			return nil, fmt.Errorf("bqHelper unsupported type %T", et)
		}

	case nil:
		return qvalue.QValueNull(qvalue.QValueKindInvalid), nil
	default:
		// If type is unsupported, return error
		return nil, fmt.Errorf("bqHelper unsupported type %T", v)
	}
}

// bqSchemaToQRecordSchema converts a bigquery schema to a QRecordSchema.
func bqSchemaToQRecordSchema(schema bigquery.Schema) qvalue.QRecordSchema {
	fields := make([]qvalue.QField, 0, len(schema))
	for _, fieldSchema := range schema {
		fields = append(fields, peer_bq.BigQueryFieldToQField(fieldSchema))
	}
	return qvalue.QRecordSchema{Fields: fields}
}

func (b *BigQueryTestHelper) ExecuteAndProcessQuery(query string) (*model.QRecordBatch, error) {
	q := b.client.Query(query)
	q.DisableQueryCache = true
	it, err := q.Read(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to run command: %w", err)
	}

	var records [][]qvalue.QValue
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
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

		records = append(records, qValues)
	}

	// Now fill column names as well. Assume the schema is retrieved from the query itself
	schema := bqSchemaToQRecordSchema(it.Schema)

	return &model.QRecordBatch{
		Schema:  schema,
		Records: records,
	}, nil
}

// returns whether the function errors or there are no nulls
func (b *BigQueryTestHelper) CheckNull(tableName string, colName []string) (bool, error) {
	if len(colName) == 0 {
		return true, nil
	}
	joinedString := strings.Join(colName, " is null or ") + " is null"
	command := fmt.Sprintf("SELECT COUNT(*) FROM `%s.%s` WHERE %s",
		b.Config.DatasetId, tableName, joinedString)
	q := b.client.Query(command)
	q.DisableQueryCache = true
	it, err := q.Read(context.Background())
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
		return false, errors.New("failed to convert row count to int64")
	}
	if cntI64 > 0 {
		return false, nil
	} else {
		return true, nil
	}
}

// check if NaN, Inf double values are null
func (b *BigQueryTestHelper) SelectRow(tableName string, cols ...string) ([]bigquery.Value, error) {
	command := fmt.Sprintf("SELECT %s FROM `%s.%s`",
		strings.Join(cols, ","), b.Config.DatasetId, tableName)
	q := b.client.Query(command)
	q.DisableQueryCache = true
	it, err := q.Read(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to run command: %w", err)
	}

	var row []bigquery.Value
	for {
		err := it.Next(&row)
		if err == iterator.Done {
			return row, nil
		}
		if err != nil {
			return nil, fmt.Errorf("failed to iterate over query results: %w", err)
		}
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
	case qvalue.QValueKindBytes:
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

func (b *BigQueryTestHelper) CreateTable(tableName string, schema *qvalue.QRecordSchema) error {
	fields := make([]string, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		bqType, err := qValueKindToBqColTypeString(field.Type)
		if err != nil {
			return err
		}
		fields = append(fields, fmt.Sprintf("`%s` %s", field.Name, bqType))
	}

	command := fmt.Sprintf("CREATE TABLE %s.%s (%s)", b.Config.DatasetId, tableName, strings.Join(fields, ", "))

	err := b.RunCommand(command)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

func (b *BigQueryTestHelper) RunInt64Query(query string) (int64, error) {
	recordBatch, err := b.ExecuteAndProcessQuery(query)
	if err != nil {
		return 0, fmt.Errorf("could not execute query: %w", err)
	}
	if len(recordBatch.Records) != 1 {
		return 0, fmt.Errorf("expected only 1 record, got %d", len(recordBatch.Records))
	}

	switch v := recordBatch.Records[0][0].(type) {
	case qvalue.QValueInt16:
		return int64(v.Val), nil
	case qvalue.QValueInt32:
		return int64(v.Val), nil
	case qvalue.QValueInt64:
		return v.Val, nil
	}
	return 0, fmt.Errorf("non-integer result: %T", recordBatch.Records[0][0])
}
