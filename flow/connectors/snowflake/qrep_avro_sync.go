package connsnowflake

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/linkedin/goavro/v2"
	log "github.com/sirupsen/logrus"
	_ "github.com/snowflakedb/gosnowflake"
)

type SnowflakeAvroSyncMethod struct {
	connector *SnowflakeConnector
	localDir  string
}

func NewSnowflakeAvroSyncMethod(connector *SnowflakeConnector, localDir string) *SnowflakeAvroSyncMethod {
	return &SnowflakeAvroSyncMethod{
		connector: connector,
		localDir:  localDir,
	}
}

func (s *SnowflakeAvroSyncMethod) SyncQRepRecords(
	flowJobName string,
	dstTableName string,
	partition *protos.QRepPartition,
	dstTableSchema []*sql.ColumnType,
	records *model.QRecordBatch) (int, error) {

	startTime := time.Now()

	// You will need to define your Avro schema as a string
	avroSchema, err := DefineAvroSchema(dstTableName, dstTableSchema)
	if err != nil {
		return 0, fmt.Errorf("failed to define Avro schema: %w", err)
	}

	fmt.Printf("Avro schema: %s\n", avroSchema)

	// Create a local file path with flowJobName and partitionID
	localFilePath := fmt.Sprintf("%s/%s.avro", s.localDir, partition.PartitionId)
	file, err := os.Create(localFilePath)
	if err != nil {
		return 0, fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Create OCF Writer
	ocfWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      file,
		Schema: avroSchema.Schema,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to create OCF writer: %w", err)
	}

	// Write each QRecord to the OCF file
	for _, qRecord := range records.Records {
		avroMap, err := qRecord.ToAvroCompatibleMap(&avroSchema.NullableFields, records.Schema.GetColumnNames())
		if err != nil {
			return 0, fmt.Errorf("failed to convert QRecord to Avro compatible map: %w", err)
		}

		err = ocfWriter.Append([]interface{}{avroMap})
		if err != nil {
			return 0, fmt.Errorf("failed to write record to OCF file: %w", err)
		}
	}

	// write this file to snowflake using COPY INTO statement
	copyCmd := fmt.Sprintf("COPY INTO %s FROM @%%%s/%s FILE_FORMAT = (TYPE = AVRO)",
		dstTableName, dstTableName, partition.PartitionId)

	if _, err = s.connector.database.Exec(copyCmd); err != nil {
		return 0, fmt.Errorf("failed to run COPY INTO command: %w", err)
	}

	// Insert metadata statement
	insertMetadataStmt, err := s.connector.createMetadataInsertStatement(partition, flowJobName, startTime)
	if err != nil {
		return -1, fmt.Errorf("failed to create metadata insert statement: %v", err)
	}

	// Execute the metadata insert statement
	if _, err = s.connector.database.Exec(insertMetadataStmt); err != nil {
		return -1, fmt.Errorf("failed to execute metadata insert statement: %v", err)
	}

	log.Printf("pushed %d records to local file %s and loaded into Snowflake table %s",
		len(records.Records), localFilePath, dstTableName)
	return len(records.Records), nil
}

type AvroField struct {
	Name string      `json:"name"`
	Type interface{} `json:"type"`
}

type AvroSchema struct {
	Type   string      `json:"type"`
	Name   string      `json:"name"`
	Fields []AvroField `json:"fields"`
}

type AvroSchemaDefinition struct {
	Schema         string
	NullableFields map[string]bool
}

func DefineAvroSchema(dstTableName string, dstTableSchema []*sql.ColumnType) (*AvroSchemaDefinition, error) {
	avroFields := []AvroField{}
	nullableFields := map[string]bool{}

	for _, sqlField := range dstTableSchema {
		avroType, err := GetAvroType(sqlField)
		if err != nil {
			return nil, err
		}

		// If a field is nullable, its Avro type should be ["null", actualType]
		nullable, ok := sqlField.Nullable()
		if !ok {
			return nil, fmt.Errorf("driver does not support the following field: %s", sqlField.Name())
		}

		if nullable {
			avroType = []interface{}{"null", avroType}
			nullableFields[sqlField.Name()] = true
		}

		avroFields = append(avroFields, AvroField{
			Name: sqlField.Name(),
			Type: avroType,
		})
	}

	avroSchema := AvroSchema{
		Type:   "record",
		Name:   dstTableName,
		Fields: avroFields,
	}

	avroSchemaJSON, err := json.Marshal(avroSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal Avro schema to JSON: %v", err)
	}

	return &AvroSchemaDefinition{
		Schema:         string(avroSchemaJSON),
		NullableFields: nullableFields,
	}, nil
}

func GetAvroType(sqlField *sql.ColumnType) (interface{}, error) {
	databaseType := sqlField.DatabaseTypeName()

	switch databaseType {
	case "VARCHAR", "CHAR", "STRING", "TEXT":
		return "string", nil
	case "BINARY":
		return "bytes", nil
	case "NUMBER":
		return map[string]interface{}{
			"type":        "bytes",
			"logicalType": "decimal",
			"precision":   38,
			"scale":       9,
		}, nil
	case "INTEGER", "BIGINT":
		return "long", nil
	case "FLOAT", "DOUBLE":
		return "double", nil
	case "BOOLEAN":
		return "boolean", nil
	case "DATE":
		return map[string]string{
			"type":        "int",
			"logicalType": "date",
		}, nil
	case "TIME":
		return map[string]string{
			"type":        "long",
			"logicalType": "time-micros",
		}, nil
	case "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ":
		return map[string]string{
			"type":        "long",
			"logicalType": "timestamp-millis",
		}, nil
	case "OBJECT", "ARRAY", "VARIANT":
		// For Snowflake semi-structured types like OBJECT, ARRAY, and VARIANT, you might need to handle it
		// separately based on the specific structure of the data.
		// If it's a simple nested structure, you can consider mapping them to "record" types in Avro, similar to
		// the bigquery.RecordFieldType case.
		return nil, fmt.Errorf("Snowflake semi-structured type %s not supported yet", databaseType)
	default:
		return nil, fmt.Errorf("unsupported Snowflake field type: %s", databaseType)
	}
}
