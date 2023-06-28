package connkafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	log "github.com/sirupsen/logrus"
)

type KafkaRecord struct {
	Before map[string]interface{} `json:"before"`
	After  map[string]interface{} `json:"after"`
}

const (
	rawTablePrefix = "_PEERDB_RAW"
)

type KafkaConnector struct {
	ctx        context.Context
	connection *kafka.Conn
	writer     *kafka.Writer
}

func NewKafkaConnector(ctx context.Context,
	kafkaProtoConfig *protos.KafkaConfig) (*KafkaConnector, error) {
	securityProtocol := kafkaProtoConfig.SecurityProtocol
	username := kafkaProtoConfig.Username
	password := kafkaProtoConfig.Password

	brokers := strings.Split(kafkaProtoConfig.Servers, ",")

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		AllowAutoTopicCreation: true,
	}

	if securityProtocol == "SASL_SSL" {
		saslMechanism := plain.Mechanism{
			Username: username,
			Password: password,
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM([]byte(kafkaProtoConfig.SslCertificate))
		tlsConfig := &tls.Config{
			InsecureSkipVerify: true,
			RootCAs:            caCertPool,
		}
		dialer.TLS = tlsConfig
		dialer.SASLMechanism = saslMechanism
		writer.Transport = &kafka.Transport{
			TLS:  tlsConfig,
			SASL: saslMechanism,
		}
	}

	conn, err := dialer.Dial("tcp", brokers[0])
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to Kafka cluster: %w", err)
	}

	log.Println("Opened connection to cluster.")

	controller, err := conn.Controller()
	if err != nil {
		return nil, fmt.Errorf("failed to get controller to leader of Kafka cluster: %w", err)
	}

	log.Println("Obtained controller of leader.")
	leaderConn := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	controllerConn, err := dialer.Dial("tcp", leaderConn)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection to leader controller: %w", err)
	}
	log.Println("Obtained controller connection.")
	writer.Addr = controllerConn.RemoteAddr()
	return &KafkaConnector{
		ctx:        ctx,
		connection: controllerConn,
		writer:     writer,
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
	return true
}

func (c *KafkaConnector) SetupMetadataTables(jobName string) error {
	metadataTopicName := "peerdb_" + jobName
	createErr := c.connection.CreateTopics(kafka.TopicConfig{
		Topic:             metadataTopicName,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})

	if createErr != nil {
		return fmt.
			Errorf("failed to create Kafka topic: %w", createErr)
	}
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

func getRawTableIdentifier(jobName string) string {
	jobName = regexp.MustCompile("[^a-zA-Z0-9]+").ReplaceAllString(jobName, "_")
	return fmt.Sprintf("%s_%s", rawTablePrefix, jobName)
}

func (c *KafkaConnector) SetupNormalizedTable(
	req *protos.SetupNormalizedTableInput) (*protos.SetupNormalizedTableOutput, error) {
	return nil, nil
}

func (c *KafkaConnector) InitializeTableSchema(req map[string]*protos.TableSchema) error {
	return nil
}

func (c *KafkaConnector) PullRecords(req *model.PullRecordsRequest) (*model.RecordBatch, error) {
	log.Errorf("panicking at call to PullRecords for Kafka flow connector")
	panic("PullRecords is not implemented for the Kafka flow connector")
}

func (c *KafkaConnector) SyncRecords(req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	destinationTopicName := getRawTableIdentifier(req.FlowJobName)
	var destinationMessage kafka.Message
	numRecords := 0
	first := true
	var firstCP int64 = 0
	lastCP := req.Records.LastCheckPointID
	for _, record := range req.Records.Records {
		switch typedRecord := record.(type) {
		case *model.InsertRecord:
			numRecords += 1
			insertData := KafkaRecord{
				Before: nil,
				After:  typedRecord.Items,
			}

			insertJSON, err := json.Marshal(insertData)
			if err != nil {
				return nil, fmt.Errorf("failed to serialize insert data to JSON: %w", err)
			}

			destinationMessage = kafka.Message{
				Topic: destinationTopicName,
				Key:   []byte("CDC"),
				Value: insertJSON,
			}

		case *model.UpdateRecord:
			numRecords += 1
			updateData := KafkaRecord{
				Before: typedRecord.OldItems,
				After:  typedRecord.NewItems,
			}
			updateJSON, err := json.Marshal(updateData)
			if err != nil {
				return nil, fmt.Errorf("failed to serialize update data to JSON: %w", err)
			}

			destinationMessage = kafka.Message{
				Topic: destinationTopicName,
				Key:   []byte("CDC"),
				Value: updateJSON,
			}

		case *model.DeleteRecord:
			numRecords += 1
			deleteData := KafkaRecord{
				Before: typedRecord.Items,
				After:  nil,
			}
			deleteJSON, err := json.Marshal(deleteData)
			if err != nil {
				return nil, fmt.Errorf("failed to serialize delete data to JSON: %w", err)
			}

			destinationMessage = kafka.Message{
				Topic: destinationTopicName,
				Key:   []byte("CDC"),
				Value: deleteJSON,
			}
		default:
			return nil, fmt.Errorf("record type %T not supported in Kafka flow connector", typedRecord)

		}
		if first {
			firstCP = record.GetCheckPointID()
			first = false
		}
	}
	if numRecords == 0 {
		return &model.SyncResponse{
			FirstSyncedCheckPointID: 0,
			LastSyncedCheckPointID:  0,
			NumRecordsSynced:        0,
		}, nil
	}

	writeErr := c.writer.WriteMessages(c.ctx, destinationMessage)

	if writeErr != nil {
		return nil, fmt.Errorf("failed to write message to Kafka topic: %w", writeErr)
	}

	return &model.SyncResponse{
		FirstSyncedCheckPointID: firstCP,
		LastSyncedCheckPointID:  lastCP,
		NumRecordsSynced:        int64(numRecords),
	}, nil
}

func (c *KafkaConnector) NormalizeRecords(req *model.NormalizeRecordsRequest) (*model.NormalizeResponse, error) {
	return &model.NormalizeResponse{
		Done:         true,
		StartBatchID: 0,
		EndBatchID:   1,
	}, nil
}

func (c *KafkaConnector) CreateRawTable(req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	destinationTopicName := getRawTableIdentifier(req.FlowJobName)
	createErr := c.connection.CreateTopics(kafka.TopicConfig{
		Topic:             destinationTopicName,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})

	if createErr != nil {
		return nil, fmt.
			Errorf("failed to create Kafka topic: %w", createErr)
	}
	return &protos.CreateRawTableOutput{
		TableIdentifier: destinationTopicName,
	}, nil
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
