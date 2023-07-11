package conns3

import (
	"fmt"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	log "github.com/sirupsen/logrus"
)

func (s *S3Connector) GetQRepPartitions(config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	panic("not implemented")
}

func (s *S3Connector) PullQRepRecords(config *protos.QRepConfig,
	partition *protos.QRepPartition,
) (*model.QRecordBatch, error) {
	panic("not implemented")
}

func (s *S3Connector) SyncQRepRecords(
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	records *model.QRecordBatch,
) (int, error) {

	dstTableName := config.DestinationTableIdentifier
	avroSchema, err := getAvroSchema(dstTableName, records.Schema)
	if err != nil {
		return 0, err
	}
	_, err = s.writeToAvroFile(records, avroSchema, partition.PartitionId, config.FlowJobName)
	if err != nil {
		return 0, err
	}
	return len(records.Records), nil
}

func getAvroSchema(
	dstTableName string,
	schema *model.QRecordSchema,
) (*model.QRecordAvroSchemaDefinition, error) {
	avroSchema, err := model.GetAvroSchemaDefinition(dstTableName, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to define Avro schema: %w", err)
	}

	return avroSchema, nil
}

func (s *S3Connector) writeToAvroFile(
	records *model.QRecordBatch,
	avroSchema *model.QRecordAvroSchemaDefinition,
	partitionID string,
	jobName string,
) (string, error) {
	s3o, err := utils.NewS3BucketAndPrefix(s.url)
	if err != nil {
		return "", fmt.Errorf("failed to parse bucket path: %w", err)
	}

	s3Key := fmt.Sprintf("%s/%s/%s.avro", s3o.Prefix, jobName, partitionID)
	err = WriteRecordsToS3(records, avroSchema, s3o.Bucket, s3Key)
	if err != nil {
		return "", fmt.Errorf("failed to write records to S3: %w", err)
	}

	return "", nil
}

// S3 just sets up destination, not metadata tables
func (s *S3Connector) SetupQRepMetadataTables(config *protos.QRepConfig) error {
	log.Infof("QRep metadata setup not needed for S3.")
	return nil
}

func (s *S3Connector) ConsolidateQRepPartitions(config *protos.QRepConfig) error {
	log.Infof("Consolidate partitions not needed for S3.")
	return nil
}

func (s *S3Connector) CleanupQRepFlow(config *protos.QRepConfig) error {
	log.Infof("Cleanup QRep Flow not needed for S3.")
	return nil
}
