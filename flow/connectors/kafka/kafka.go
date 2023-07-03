package connkafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	log "github.com/sirupsen/logrus"
)

type KafkaRecord struct {
	Before map[string]interface{} `json:"before"`
	After  map[string]interface{} `json:"after"`
}

type KafkaConnector struct {
	ctx      context.Context
	client   *kafka.AdminClient
	consumer *kafka.Consumer
	producer *kafka.Producer
}

func NewKafkaConnector(ctx context.Context,
	kafkaProtoConfig *protos.KafkaConfig) (*KafkaConnector, error) {
	brokers := kafkaProtoConfig.Servers
	connectorConfig := kafka.ConfigMap{
		"bootstrap.servers":        brokers,
		"allow.auto.create.topics": true,
	}
	securityProtocol := kafkaProtoConfig.SecurityProtocol

	if securityProtocol == "SASL_SSL" {
		rootCertToVerifyBroker := kafkaProtoConfig.SslCertificate
		connectorConfig.SetKey("security.protocol", "SASL_SSL")
		connectorConfig.SetKey("ssl.ca.pem", rootCertToVerifyBroker)
		connectorConfig.SetKey("sasl.mechanism", "PLAIN")
		connectorConfig.SetKey("sasl.username", kafkaProtoConfig.Username)
		connectorConfig.SetKey("sasl.password", kafkaProtoConfig.Password)
	}

	producerConfig := &kafka.ConfigMap{
		"transactional.id": "peerdb",
	}
	consumerConfig := &kafka.ConfigMap{
		"group.id":          "unused but needed",
		"auto.offset.reset": "latest",
	}
	// Maintaining separate configs for consumer and producer.
	// We get warnings otherwise in the logs.
	for key, value := range connectorConfig {
		(*consumerConfig)[key] = value
		(*producerConfig)[key] = value
	}
	producer, err := kafka.NewProducer(producerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	consumer, err := kafka.NewConsumer(consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	client, err := kafka.NewAdminClient(&connectorConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}
	log.Infoln("Client, consumer and producer ready")

	return &KafkaConnector{
		ctx:      ctx,
		client:   client,
		consumer: consumer,
		producer: producer,
	}, nil
}

func (c *KafkaConnector) Close() error {
	if c == nil || c.client == nil {
		return nil
	}
	c.client.Close()
	return nil
}

func (c *KafkaConnector) ConnectionActive() bool {
	if c == nil || c.client == nil {
		return false
	}
	_, err := c.client.GetMetadata(nil, true, 5000)
	if err != nil {
		log.Errorf("failed to get metadata from connection: %s", err)
	}
	return err != nil
}

func (c *KafkaConnector) NeedsSetupMetadataTables(jobName string) bool {
	metadataTopicName := "peerdb_" + jobName
	_, err := c.client.GetMetadata(&metadataTopicName, false, 5000)
	if err != nil {
		log.Errorf("failed to check if metadata topic exists: %s", err)
		return true
	}
	return false
}

func (c *KafkaConnector) SetupMetadataTables(jobName string) error {
	metadataTopicName := "peerdb_" + jobName
	topicResults, createErr := c.client.CreateTopics(c.ctx, []kafka.TopicSpecification{
		{
			Topic:             metadataTopicName,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	})
	if createErr != nil {
		return fmt.
			Errorf("failed client's topics creation: %w", createErr)
	}
	topicErr := topicResults[0].Error.Code().String()
	if topicErr != "ErrNoError" {
		if topicExists(topicErr) {
			return nil
		}
		return fmt.
			Errorf("failed to create metadata topic: %s", topicErr)
	}
	return nil
}

func (c *KafkaConnector) GetLastOffset(jobName string) (*protos.LastSyncState, error) {
	metadataTopicName := "peerdb_" + jobName

	subscribeErr := c.consumer.SubscribeTopics([]string{metadataTopicName}, nil)
	if subscribeErr != nil {
		return nil, fmt.Errorf("failed to subscribe offset reader to metadata topic: %w", subscribeErr)
	}

	assignErr := c.consumer.Assign([]kafka.TopicPartition{{
		Topic:     &metadataTopicName,
		Partition: 0,
		Offset:    kafka.OffsetTail(2), // not 1 because ReadMessage reads the next message, not current
	}})
	if assignErr != nil {
		return nil, fmt.Errorf("failed to assign partition for offset reader: %w", assignErr)
	}

	lastMessage, readErr := c.consumer.ReadMessage(60 * time.Second)
	if readErr != nil {
		if strings.Contains(readErr.Error(), "Timed out") {
			log.Warnf("offset reader timed out. Assuming this means this is the first sync for this mirror")
			return nil, nil
		}
		return nil, fmt.Errorf("unable to read latest offset: %w", readErr)
	}

	lastCheckpoint, integerParseErr := strconv.ParseInt(string(lastMessage.Value), 10, 64)
	if integerParseErr != nil {
		return nil, fmt.Errorf("error converting checkpoint string to int64: %w", integerParseErr)
	}

	if err := c.consumer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close offset reader: %w", err)
	}

	return &protos.LastSyncState{
		Checkpoint: lastCheckpoint,
	}, nil
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
func topicExists(err string) bool {
	return strings.Contains(err, "Topic already exists")
}

func (c *KafkaConnector) SetupNormalizedTable(
	req *protos.SetupNormalizedTableInput) (*protos.SetupNormalizedTableOutput, error) {
	destinationTopicName := "peerdb_" + req.SourceTableSchema.TableIdentifier
	alreadyExists := &protos.SetupNormalizedTableOutput{
		TableIdentifier: destinationTopicName,
		AlreadyExists:   true,
	}
	topicResults, createErr := c.client.CreateTopics(c.ctx, []kafka.TopicSpecification{
		{
			Topic:             destinationTopicName,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	})
	if createErr != nil {
		if topicExists(createErr.Error()) {
			return alreadyExists, nil
		}
		return nil, fmt.
			Errorf("failed client's topics creation: %w", createErr)
	}

	topicErr := topicResults[0].Error.Code().String()
	if topicErr != "ErrNoError" {
		if topicExists(topicErr) {
			return alreadyExists, nil
		}
		return nil, fmt.
			Errorf("failed to create destination topic: %s", topicErr)
	}

	return &protos.SetupNormalizedTableOutput{
		TableIdentifier: destinationTopicName,
		AlreadyExists:   false,
	}, nil
}

func (c *KafkaConnector) InitializeTableSchema(req map[string]*protos.TableSchema) error {
	log.Info("kafka does not require table schema initialisation.")
	return nil
}

func (c *KafkaConnector) PullRecords(req *model.PullRecordsRequest) (*model.RecordBatch, error) {
	log.Errorf("panicking at call to PullRecords for Kafka flow connector")
	panic("PullRecords is not implemented for the Kafka flow connector")
}

func (c *KafkaConnector) SyncRecords(req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	var destinationTopicName string
	noRecordResponse := &model.SyncResponse{
		FirstSyncedCheckPointID: 0,
		LastSyncedCheckPointID:  0,
		NumRecordsSynced:        0,
	}
	if len(req.Records.Records) > 0 {
		destinationTopicName = "peerdb_" + req.Records.Records[0].GetTableName()
	} else {
		return noRecordResponse, nil
	}
	var destinationMessage kafka.Message

	first := true
	var firstCP int64 = 0
	lastCP := req.Records.LastCheckPointID
	records := make([]kafka.Message, 0)
	destinationTopic := kafka.TopicPartition{
		Topic:     &destinationTopicName,
		Partition: kafka.PartitionAny,
	}

	for _, record := range req.Records.Records {
		switch typedRecord := record.(type) {
		case *model.InsertRecord:
			insertData := KafkaRecord{
				Before: nil,
				After:  typedRecord.Items,
			}

			insertJSON, err := json.Marshal(insertData)
			if err != nil {
				return nil, fmt.Errorf("failed to serialize insert data to JSON: %w", err)
			}

			destinationMessage = kafka.Message{
				TopicPartition: destinationTopic,
				Key:            []byte("CDC"),
				Value:          insertJSON,
			}

		case *model.UpdateRecord:
			updateData := KafkaRecord{
				Before: typedRecord.OldItems,
				After:  typedRecord.NewItems,
			}
			updateJSON, err := json.Marshal(updateData)
			if err != nil {
				return nil, fmt.Errorf("failed to serialize update data to JSON: %w", err)
			}

			destinationMessage = kafka.Message{
				TopicPartition: destinationTopic,
				Key:            []byte("CDC"),
				Value:          updateJSON,
			}

		case *model.DeleteRecord:
			deleteData := KafkaRecord{
				Before: typedRecord.Items,
				After:  nil,
			}

			deleteJSON, err := json.Marshal(deleteData)
			if err != nil {
				return nil, fmt.Errorf("failed to serialize delete data to JSON: %w", err)
			}

			destinationMessage = kafka.Message{
				TopicPartition: destinationTopic,
				Key:            []byte("CDC"),
				Value:          deleteJSON,
			}
		default:
			return nil, fmt.Errorf("record type %T not supported in Kafka flow connector", typedRecord)
		}
		records = append(records, destinationMessage)

		if first {
			firstCP = record.GetCheckPointID()
			first = false
		}
	}
	if len(records) == 0 {
		return noRecordResponse, nil
	}
	metadataTopicName := "peerdb_" + req.FlowJobName
	metadataTopic := kafka.TopicPartition{
		Topic:     &metadataTopicName,
		Partition: kafka.PartitionAny,
	}
	checkpointBytes := []byte(strconv.FormatInt(lastCP, 10))

	initErr := c.producer.InitTransactions(c.ctx)
	if initErr != nil {
		return nil, fmt.Errorf("failed to initialise transaction: %w", initErr)
	}

	beginErr := c.producer.BeginTransaction()
	if beginErr != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", beginErr)
	}

	writeErr := c.producer.Produce(&destinationMessage, nil)
	if writeErr != nil {
		abortErr := c.producer.AbortTransaction(c.ctx)
		if abortErr != nil {
			return nil, fmt.Errorf("destination write failed, but could not abort transaction: %w", abortErr)
		}
	}

	updateErr := c.producer.Produce(&kafka.Message{
		TopicPartition: metadataTopic,
		Key:            []byte("checkpoint"),
		Value:          checkpointBytes,
	}, nil)
	if updateErr != nil {
		abortErr := c.producer.AbortTransaction(c.ctx)
		if abortErr != nil {
			return nil, fmt.Errorf("checkpoint update failed, but could not abort transaction: %w", abortErr)
		}
	}

	commitErr := c.producer.CommitTransaction(c.ctx)
	if commitErr != nil {
		return nil, fmt.Errorf("could not commit transaction: %w", commitErr)
	}

	return &model.SyncResponse{
		FirstSyncedCheckPointID: firstCP,
		LastSyncedCheckPointID:  lastCP,
		NumRecordsSynced:        int64(len(records)),
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
