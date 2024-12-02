package connbigquery

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"cloud.google.com/go/bigquery"

	"github.com/PeerDB-io/peer-flow/datatypes"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
)

func (c *BigQueryConnector) SyncQRepRecords(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int, error) {
	// Ensure the destination table is available.
	destTable := config.DestinationTableIdentifier
	srcSchema := stream.Schema()

	tblMetadata, err := c.replayTableSchemaDeltasQRep(ctx, config, partition, srcSchema)
	if err != nil {
		return 0, err
	}

	c.logger.Info(fmt.Sprintf("QRep sync function called and partition existence checked for"+
		" partition %s of destination table %s",
		partition.PartitionId, destTable))

	avroSync := NewQRepAvroSyncMethod(c, config.StagingPath, config.FlowJobName)
	return avroSync.SyncQRepRecords(ctx, config.Env, config.FlowJobName, destTable, partition,
		tblMetadata, stream, config.SyncedAtColName, config.SoftDeleteColName)
}

func (c *BigQueryConnector) replayTableSchemaDeltasQRep(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	srcSchema qvalue.QRecordSchema,
) (*bigquery.TableMetadata, error) {
	destDatasetTable, _ := c.convertToDatasetTable(config.DestinationTableIdentifier)
	bqTable := c.client.DatasetInProject(c.projectID, destDatasetTable.dataset).Table(destDatasetTable.table)
	dstTableMetadata, err := bqTable.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata of table %s: %w", destDatasetTable, err)
	}

	tableSchemaDelta := &protos.TableSchemaDelta{
		SrcTableName: config.WatermarkTable,
		DstTableName: config.DestinationTableIdentifier,
	}

	for _, col := range srcSchema.Fields {
		hasColumn := false
		// check ignoring case
		for _, dstCol := range dstTableMetadata.Schema {
			if strings.EqualFold(col.Name, dstCol.Name) {
				hasColumn = true
				break
			}
		}

		if !hasColumn {
			c.logger.Info(fmt.Sprintf("adding column %s to destination table %s",
				col.Name, config.DestinationTableIdentifier),
				slog.String(string(shared.PartitionIDKey), partition.PartitionId))
			tableSchemaDelta.AddedColumns = append(tableSchemaDelta.AddedColumns, &protos.FieldDescription{
				Name:         col.Name,
				Type:         string(col.Type),
				TypeModifier: datatypes.MakeNumericTypmod(int32(col.Precision), int32(col.Scale)),
			},
			)
		}
	}

	err = c.ReplayTableSchemaDeltas(ctx, config.Env, config.FlowJobName, []*protos.TableSchemaDelta{tableSchemaDelta})
	if err != nil {
		return nil, fmt.Errorf("failed to add columns to destination table: %w", err)
	}
	dstTableMetadata, err = bqTable.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata of table %s: %w", destDatasetTable, err)
	}
	return dstTableMetadata, nil
}

func (c *BigQueryConnector) SetupQRepMetadataTables(ctx context.Context, config *protos.QRepConfig) error {
	if config.WriteMode.WriteType == protos.QRepWriteType_QREP_WRITE_MODE_OVERWRITE {
		query := c.queryWithLogging("TRUNCATE TABLE " + config.DestinationTableIdentifier)
		query.DefaultDatasetID = c.datasetID
		query.DefaultProjectID = c.projectID
		_, err := query.Read(ctx)
		if err != nil {
			return fmt.Errorf("failed to TRUNCATE table before query replication: %w", err)
		}
	}

	return nil
}
