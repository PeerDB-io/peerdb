package conns3

import (
	"context"
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/connectors/utils/metrics"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type S3Connector struct {
	ctx        context.Context
	url        string
	pgMetadata *PostgresMetadataStore
	client     s3.S3
	creds      utils.S3PeerCredentials
}

func NewS3Connector(ctx context.Context,
	config *protos.S3Config) (*S3Connector, error) {
	keyID := ""
	if config.AccessKeyId != nil {
		keyID = *config.AccessKeyId
	}
	secretKey := ""
	if config.SecretAccessKey != nil {
		secretKey = *config.SecretAccessKey
	}
	roleArn := ""
	if config.RoleArn != nil {
		roleArn = *config.RoleArn
	}
	region := ""
	if config.Region != nil {
		region = *config.Region
	}
	endpoint := ""
	if config.Endpoint != nil {
		endpoint = *config.Endpoint
	}
	s3PeerCreds := utils.S3PeerCredentials{
		AccessKeyID:     keyID,
		SecretAccessKey: secretKey,
		AwsRoleArn:      roleArn,
		Region:          region,
		Endpoint:        endpoint,
	}
	s3Client, err := utils.CreateS3Client(s3PeerCreds)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}
	pgMetadata, err := NewPostgresMetadataStore(ctx, config.GetMetadataDb())
	if err != nil {
		log.Errorf("failed to create postgres metadata store: %v", err)
		return nil, err
	}

	return &S3Connector{
		ctx:        ctx,
		url:        config.Url,
		pgMetadata: pgMetadata,
		client:     *s3Client,
		creds:      s3PeerCreds,
	}, nil
}

func (c *S3Connector) Close() error {
	log.Debugf("Closing s3 connector is a noop")
	return nil
}

func (c *S3Connector) SyncRecords(req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	if len(req.Records.Records) == 0 {
		return &model.SyncResponse{
			FirstSyncedCheckPointID: 0,
			LastSyncedCheckPointID:  0,
			NumRecordsSynced:        0,
		}, nil
	}

	syncBatchID, err := c.GetLastSyncBatchID(req.FlowJobName)
	if err != nil {
		return nil, fmt.Errorf("failed to get previous syncBatchID: %w", err)
	}
	syncBatchID = syncBatchID + 1
	lastCP := req.Records.LastCheckPointID
	recordStream := model.NewQRecordStream(len(req.Records.Records))

	err = recordStream.SetSchema(&model.QRecordSchema{
		Fields: []*model.QField{
			{
				Name:     "_peerdb_uid",
				Type:     qvalue.QValueKindString,
				Nullable: false,
			},
			{
				Name:     "_peerdb_timestamp",
				Type:     qvalue.QValueKindInt64,
				Nullable: false,
			},
			{
				Name:     "_peerdb_destination_table_name",
				Type:     qvalue.QValueKindString,
				Nullable: false,
			},
			{
				Name:     "_peerdb_data",
				Type:     qvalue.QValueKindString,
				Nullable: false,
			},
			{
				Name:     "_peerdb_record_type",
				Type:     qvalue.QValueKindInt64,
				Nullable: true,
			},
			{
				Name:     "_peerdb_match_data",
				Type:     qvalue.QValueKindString,
				Nullable: true,
			},
			{
				Name:     "_peerdb_batch_id",
				Type:     qvalue.QValueKindInt64,
				Nullable: true,
			},
			{
				Name:     "_peerdb_unchanged_toast_columns",
				Type:     qvalue.QValueKindString,
				Nullable: true,
			},
		},
	})
	if err != nil {
		return nil, err
	}

	first := true
	var firstCP int64 = 0
	tableNameRowsMapping := make(map[string]uint32)

	for _, record := range req.Records.Records {
		var entries [8]qvalue.QValue
		switch typedRecord := record.(type) {
		case *model.InsertRecord:
			// json.Marshal converts bytes in Hex automatically to BASE64 string.
			itemsJSON, err := typedRecord.Items.ToJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to serialize insert record items to JSON: %w", err)
			}

			// add insert record to the raw table
			entries[2] = qvalue.QValue{
				Kind:  qvalue.QValueKindString,
				Value: typedRecord.DestinationTableName,
			}
			entries[3] = qvalue.QValue{
				Kind:  qvalue.QValueKindString,
				Value: itemsJSON,
			}
			entries[4] = qvalue.QValue{
				Kind:  qvalue.QValueKindInt64,
				Value: 0,
			}
			entries[5] = qvalue.QValue{
				Kind:  qvalue.QValueKindString,
				Value: "",
			}
			entries[7] = qvalue.QValue{
				Kind:  qvalue.QValueKindString,
				Value: utils.KeysToString(typedRecord.UnchangedToastColumns),
			}
			tableNameRowsMapping[typedRecord.DestinationTableName] += 1
		case *model.UpdateRecord:
			newItemsJSON, err := typedRecord.NewItems.ToJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to serialize update record new items to JSON: %w", err)
			}
			oldItemsJSON, err := typedRecord.OldItems.ToJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to serialize update record old items to JSON: %w", err)
			}

			entries[2] = qvalue.QValue{
				Kind:  qvalue.QValueKindString,
				Value: typedRecord.DestinationTableName,
			}
			entries[3] = qvalue.QValue{
				Kind:  qvalue.QValueKindString,
				Value: newItemsJSON,
			}
			entries[4] = qvalue.QValue{
				Kind:  qvalue.QValueKindInt64,
				Value: 1,
			}
			entries[5] = qvalue.QValue{
				Kind:  qvalue.QValueKindString,
				Value: oldItemsJSON,
			}
			entries[7] = qvalue.QValue{
				Kind:  qvalue.QValueKindString,
				Value: utils.KeysToString(typedRecord.UnchangedToastColumns),
			}
			tableNameRowsMapping[typedRecord.DestinationTableName] += 1
		case *model.DeleteRecord:
			itemsJSON, err := typedRecord.Items.ToJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to serialize delete record items to JSON: %w", err)
			}

			// append delete record to the raw table
			entries[2] = qvalue.QValue{
				Kind:  qvalue.QValueKindString,
				Value: typedRecord.DestinationTableName,
			}
			entries[3] = qvalue.QValue{
				Kind:  qvalue.QValueKindString,
				Value: itemsJSON,
			}
			entries[4] = qvalue.QValue{
				Kind:  qvalue.QValueKindInt64,
				Value: 2,
			}
			entries[5] = qvalue.QValue{
				Kind:  qvalue.QValueKindString,
				Value: itemsJSON,
			}
			entries[7] = qvalue.QValue{
				Kind:  qvalue.QValueKindString,
				Value: utils.KeysToString(typedRecord.UnchangedToastColumns),
			}
			tableNameRowsMapping[typedRecord.DestinationTableName] += 1
		default:
			return nil, fmt.Errorf("record type %T not supported in Snowflake flow connector", typedRecord)
		}

		if first {
			firstCP = record.GetCheckPointID()
			first = false
		}

		entries[0] = qvalue.QValue{
			Kind:  qvalue.QValueKindString,
			Value: uuid.New().String(),
		}
		entries[1] = qvalue.QValue{
			Kind:  qvalue.QValueKindInt64,
			Value: time.Now().UnixNano(),
		}
		entries[6] = qvalue.QValue{
			Kind:  qvalue.QValueKindInt64,
			Value: syncBatchID,
		}

		recordStream.Records <- &model.QRecordOrError{
			Record: &model.QRecord{
				NumEntries: 8,
				Entries:    entries[:],
			},
		}
	}

	qrepConfig := &protos.QRepConfig{
		FlowJobName:                req.FlowJobName,
		DestinationTableIdentifier: fmt.Sprintf("raw_table_%s", req.FlowJobName),
	}
	partition := &protos.QRepPartition{
		PartitionId: fmt.Sprint(syncBatchID),
	}
	startTime := time.Now()
	close(recordStream.Records)
	numRecords, err := c.SyncQRepRecords(qrepConfig, partition, recordStream)
	if err != nil {
		return nil, err
	}

	err = c.updateLastOffset(req.FlowJobName, lastCP)
	if err != nil {
		log.Errorf("failed to update last offset for s3 cdc: %v", err)
		return nil, err
	}
	err = c.incrementSyncBatchID(req.FlowJobName)
	if err != nil {
		log.Errorf("%v", err)
		return nil, err
	}
	metrics.LogSyncMetrics(c.ctx, req.FlowJobName, int64(numRecords), time.Since(startTime))
	return &model.SyncResponse{
		FirstSyncedCheckPointID: firstCP,
		LastSyncedCheckPointID:  lastCP,
		NumRecordsSynced:        int64(numRecords),
		TableNameRowsMapping:    tableNameRowsMapping,
	}, nil
}

func (c *S3Connector) CreateRawTable(req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	log.Infof("CreateRawTable for S3 is a no-op")
	return nil, nil
}

func (c *S3Connector) InitializeTableSchema(req map[string]*protos.TableSchema) error {
	log.Infof("InitializeTableSchema for S3 is a no-op")
	return nil
}

func (c *S3Connector) SetupNormalizedTables(req *protos.SetupNormalizedTableBatchInput) (
	*protos.SetupNormalizedTableBatchOutput,
	error) {
	log.Infof("SetupNormalizedTables for S3 is a no-op")
	return nil, nil
}

func (c *S3Connector) ConnectionActive() bool {
	_, err := c.client.ListBuckets(nil)
	return err == nil
}
