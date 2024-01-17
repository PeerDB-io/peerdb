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
	"github.com/PeerDB-io/peer-flow/shared"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaRecord struct {
	Before model.RecordItems `json:"before"`
	After  model.RecordItems `json:"after"`
}

type KafkaConnector struct {
	ctx      context.Context
	client   *kafka.AdminClient
	consumer *kafka.Consumer
	producer *kafka.Producer
}

func NewKafkaConnector(ctx context.Context,
	kafkaProtoConfig *protos.KafkaConfig,
) (*KafkaConnector, error) {
	brokers := kafkaProtoConfig.Servers
	connectorConfig := kafka.ConfigMap{
		"bootstrap.servers":        brokers,
		"allow.auto.create.topics": true,
	}
	securityProtocol := kafkaProtoConfig.SecurityProtocol

	if securityProtocol == "SASL_SSL" {
		rootCertToVerifyBroker := kafkaProtoConfig.SslCertificate
		sslConfig := kafka.ConfigMap{
			"security.protocol": "SASL_SSL",
			"ssl.ca.location":   rootCertToVerifyBroker,
			"sasl.mechanisms":   "PLAIN",
			"sasl.username":     kafkaProtoConfig.Username,
			"sasl.password":     kafkaProtoConfig.Password,
		}

		for key, value := range sslConfig {
			(connectorConfig)[key] = value
		}
	}

	producerConfig := &kafka.ConfigMap{
		"transactional.id": "peerdb",
	}
	consumerConfig := &kafka.ConfigMap{
		"group.id":          "unused but needed",
		"auto.offset.reset": "latest",
	}
	// Maintaining separate configs for consumer and producer.
	// Otherwise, we get warnings in the logs.
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

func (c *KafkaConnector) ConnectionActive() error {
	if c == nil || c.client == nil {
		return fmt.Errorf("kafka client is nil")
	}
	_, err := c.client.GetMetadata(nil, true, 5000)
	return err
}

func (c *KafkaConnector) NeedsSetupMetadataTables() bool {
	jobName, ok := c.ctx.Value(shared.FlowNameKey).(string)
	if !ok {
		return false
	}

	metadataTopicName := "peerdb_" + jobName
	_, err := c.client.GetMetadata(&metadataTopicName, false, 5000)
	return err != nil
}

func (c *KafkaConnector) SetupMetadataTables() error {
	jobName, ok := c.ctx.Value(shared.FlowNameKey).(string)
	if !ok {
		return fmt.Errorf("failed to get job name from context")
	}

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

func topicExists(err string) bool {
	return strings.Contains(err, "Topic already exists")
}

func (c *KafkaConnector) SetupNormalizedTables(
	req *protos.SetupNormalizedTableBatchInput,
) (*protos.SetupNormalizedTableBatchOutput, error) {
	tableExistsMapping := make(map[string]bool)
	for tableIdentifier := range req.TableNameSchemaMapping {
		topicResults, createErr := c.client.CreateTopics(c.ctx, []kafka.TopicSpecification{
			{
				Topic:             tableIdentifier,
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
		})
		if createErr != nil {
			if topicExists(createErr.Error()) {
				tableExistsMapping[tableIdentifier] = true
				continue
			}
			return nil, fmt.
				Errorf("failed client's topics creation: %w", createErr)
		}

		topicErr := topicResults[0].Error.Code().String()
		if topicErr != "ErrNoError" {
			if topicExists(topicErr) {
				tableExistsMapping[tableIdentifier] = true
				continue
			}
			return nil, fmt.
				Errorf("failed to create destination topic: %s", topicErr)
		}
		tableExistsMapping[tableIdentifier] = false
	}

	return &protos.SetupNormalizedTableBatchOutput{
		TableExistsMapping: tableExistsMapping,
	}, nil
}

func (c *KafkaConnector) InitializeTableSchema(req map[string]*protos.TableSchema) error {
	return nil
}

func (c *KafkaConnector) SyncRecords(req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	var destinationMessage kafka.Message
	lastCP, err := req.Records.GetLastCheckpoint()
	if err != nil {
		return nil, err
	}

	records := make([]kafka.Message, 0)
	for record := range req.Records.GetRecords() {
		switch typedRecord := record.(type) {
		case *model.InsertRecord:
			insertData := KafkaRecord{
				Before: model.RecordItems{},
				After:  *typedRecord.Items,
			}

			insertJSON, err := json.Marshal(insertData)
			if err != nil {
				return nil, fmt.Errorf("failed to serialize insert data to JSON: %w", err)
			}

			destinationMessage = kafka.Message{
				Value: insertJSON,
			}

		case *model.UpdateRecord:
			updateData := KafkaRecord{
				Before: *typedRecord.OldItems,
				After:  *typedRecord.NewItems,
			}
			updateJSON, err := json.Marshal(updateData)
			if err != nil {
				return nil, fmt.Errorf("failed to serialize update data to JSON: %w", err)
			}

			destinationMessage = kafka.Message{
				Value: updateJSON,
			}

		case *model.DeleteRecord:
			deleteData := KafkaRecord{
				Before: *typedRecord.Items,
				After:  model.RecordItems{},
			}

			deleteJSON, err := json.Marshal(deleteData)
			if err != nil {
				return nil, fmt.Errorf("failed to serialize delete data to JSON: %w", err)
			}

			destinationMessage = kafka.Message{
				Value: deleteJSON,
			}
		default:
			return nil, fmt.Errorf("record type %T not supported in Kafka flow connector", typedRecord)
		}
		destinationTopicName := "peerdb_" + record.GetDestinationTableName()
		destinationTopic := kafka.TopicPartition{
			Topic:     &destinationTopicName,
			Partition: kafka.PartitionAny,
		}
		destinationMessage.TopicPartition = destinationTopic
		destinationMessage.Key = []byte("CDC")
		records = append(records, destinationMessage)
	}
	if len(records) == 0 {
		return &model.SyncResponse{
			LastSyncedCheckPointID: 0,
			NumRecordsSynced:       0,
		}, nil
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

	for _, record := range records {
		pushedRecord := record
		writeErr := c.producer.Produce(&pushedRecord, nil)
		if writeErr != nil {
			abortErr := c.producer.AbortTransaction(c.ctx)
			if abortErr != nil {
				return nil, fmt.Errorf("destination write failed, but could not abort transaction: %w", abortErr)
			}
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
		LastSyncedCheckPointID: lastCP,
		NumRecordsSynced:       int64(len(records)),
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

func (c *KafkaConnector) SyncFlowCleanup(jobName string) error {
	return nil
}
