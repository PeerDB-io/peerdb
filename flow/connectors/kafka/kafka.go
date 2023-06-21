package connkafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	log "github.com/sirupsen/logrus"
)

type KafkaConnector struct {
	ctx        context.Context
	connection *kafka.Conn
}

func NewKafkaConnector(ctx context.Context,
	kafkaProtoConfig *protos.KafkaConfig) (*KafkaConnector, error) {
	username := kafkaProtoConfig.Username
	password := kafkaProtoConfig.Password
	saslMechanism := plain.Mechanism{
		Username: username,
		Password: password,
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM([]byte(kafkaProtoConfig.SslCertificate))
	broker := kafkaProtoConfig.Servers
	tlsConfig := &tls.Config{
		InsecureSkipVerify: false,
		RootCAs:            caCertPool,
	}
	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		TLS:           tlsConfig,
		SASLMechanism: saslMechanism,
	}
	conn, err := dialer.DialContext(ctx, "tcp", broker)

	if err != nil {
		return nil, fmt.Errorf("failed to open connection to Kafka cluster: %w", err)
	}

	return &KafkaConnector{
		ctx:        ctx,
		connection: conn,
	}, nil
}

func (c *KafkaConnector) Close() error {
	if c == nil || c.connection == nil {
		return nil
	}
	err := c.connection.Close()
	if err != nil {
		return fmt.Errorf("error while closing connection to Kafka peer: %w", err)
	}
	return nil
}

func (c *KafkaConnector) ConnectionActive() bool {
	if c == nil || c.connection == nil {
		return false
	}
	_, err := c.connection.ReadPartitions()
	return err != nil
}

func (c *KafkaConnector) NeedsSetupMetadataTables() bool {
	return false
}

func (c *KafkaConnector) SetupMetadataTables() error {
	return nil
}

func (c *KafkaConnector) GetLastOffset(jobName string) (*protos.LastSyncState, error) {
	return nil, nil
}

func (c *KafkaConnector) GetLastSyncBatchID(jobName string) (int64, error) {
	return -1, nil
}

func (c *KafkaConnector) GetLastNormalizeBatchID(jobName string) (int64, error) {
	log.Errorf("panicking at call to GetLastNormalizeBatchID for Kafka flow connector")
	panic("GetLastNormalizeBatchID is not implemented for the Kafka flow connector")
}

func (c *KafkaConnector) GetTableSchema(req *protos.GetTableSchemaInput) (*protos.TableSchema, error) {
	log.Errorf("panicking at call to GetTableSchema for Kafka flow connector")
	panic("GetTableSchema is not implemented for the Kafka flow connector")
}

func (c *KafkaConnector) SetupNormalizedTable(
	req *protos.SetupNormalizedTableInput) (*protos.SetupNormalizedTableOutput, error) {
	log.Errorf("panicking at call to SetupNormalizedTable for Kafka flow connector")
	panic("SetupNormalizedTable is not implemented for the Kafka flow connector")

}

func (c *KafkaConnector) InitializeTableSchema(req map[string]*protos.TableSchema) error {
	return nil
}

func (c *KafkaConnector) PullRecords(req *model.PullRecordsRequest) (*model.RecordBatch, error) {
	log.Errorf("panicking at call to PullRecords for Kafka flow connector")
	panic("PullRecords is not implemented for the Kafka flow connector")
}

func (c *KafkaConnector) SyncRecords(req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	first := true
	var firstCP int64 = 0
	lastCP := req.Records.LastCheckPointID
	for _, record := range req.Records.Records {
		switch typedRecord := record.(type) {
		case *model.InsertRecord:
			itemsJSON, err := json.Marshal(typedRecord.Items)
			if err != nil {
				return nil, fmt.Errorf("failed to serialize insert record items to JSON: %w", err)
			}
			destinationTopicName := "peerdb_" + typedRecord.DestinationTableName
			c.connection.CreateTopics(kafka.TopicConfig{
				Topic: destinationTopicName,
			})

			bytesWritten, err := c.connection.WriteMessages(kafka.Message{
				Topic: destinationTopicName,
				Key:   []byte("data"),
				Value: itemsJSON,
			})

			if err != nil {
				return nil, fmt.Errorf("failed to message to Kafka topic: %w", err)
			}

			log.Debug("%w bytes written to Kafka topic %w.", bytesWritten, destinationTopicName)
		}
		if first {
			firstCP = record.GetCheckPointID()
			first = false
		}
	}
	return &model.SyncResponse{
		FirstSyncedCheckPointID: firstCP,
		LastSyncedCheckPointID:  lastCP,
		NumRecordsSynced:        int64(len(req.Records.Records)),
	}, nil
}

func (c *KafkaConnector) NormalizeRecords(req *model.NormalizeRecordsRequest) (*model.NormalizeResponse, error) {
	log.Errorf("panicking at call to NormalizeRecords for Kafka flow connector")
	panic("NormalizeRecords is not implemented for the Kafka flow connector")
}

func (c *KafkaConnector) CreateRawTable(req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	return nil, nil
}

func (c *KafkaConnector) EnsurePullability(req *protos.EnsurePullabilityInput,
) (*protos.EnsurePullabilityOutput, error) {
	log.Errorf("panicking at call to EnsurePullability for Kafka flow connector")
	panic("EnsurePullability is not implemented for the Kafka flow connector")
}

func (c *KafkaConnector) SetupReplication(req *protos.SetupReplicationInput) error {
	log.Errorf("panicking at call to SetupReplication for Kafka flow connector")
	panic("SetupReplication is not implemented for the Kafka flow connector")
}

func (c *KafkaConnector) PullFlowCleanup(jobName string) error {
	log.Errorf("panicking at call to PullFlowCleanup for Kafka flow connector")
	panic("PullFlowCleanup is not implemented for the Kafka flow connector")
}

func (c *KafkaConnector) SyncFlowCleanup(jobName string) error {

	return nil
}
