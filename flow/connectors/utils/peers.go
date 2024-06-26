package utils

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

func CreatePeerNoValidate(
	ctx context.Context,
	pool *pgxpool.Pool,
	peer *protos.Peer,
) (*protos.CreatePeerResponse, error) {
	config := peer.Config
	peerType := peer.Type
	wrongConfigResponse := &protos.CreatePeerResponse{
		Status: protos.CreatePeerStatus_FAILED,
		Message: fmt.Sprintf("invalid config for %s peer %s",
			peerType, peer.Name),
	}
	var innerConfig proto.Message
	switch peerType {
	case protos.DBType_POSTGRES:
		pgConfigObject, ok := config.(*protos.Peer_PostgresConfig)
		if !ok {
			return wrongConfigResponse, nil
		}
		innerConfig = pgConfigObject.PostgresConfig
	case protos.DBType_SNOWFLAKE:
		sfConfigObject, ok := config.(*protos.Peer_SnowflakeConfig)
		if !ok {
			return wrongConfigResponse, nil
		}
		innerConfig = sfConfigObject.SnowflakeConfig
	case protos.DBType_BIGQUERY:
		bqConfigObject, ok := config.(*protos.Peer_BigqueryConfig)
		if !ok {
			return wrongConfigResponse, nil
		}
		innerConfig = bqConfigObject.BigqueryConfig
	case protos.DBType_SQLSERVER:
		sqlServerConfigObject, ok := config.(*protos.Peer_SqlserverConfig)
		if !ok {
			return wrongConfigResponse, nil
		}
		innerConfig = sqlServerConfigObject.SqlserverConfig
	case protos.DBType_S3:
		s3ConfigObject, ok := config.(*protos.Peer_S3Config)
		if !ok {
			return wrongConfigResponse, nil
		}
		innerConfig = s3ConfigObject.S3Config
	case protos.DBType_CLICKHOUSE:
		chConfigObject, ok := config.(*protos.Peer_ClickhouseConfig)
		if !ok {
			return wrongConfigResponse, nil
		}
		innerConfig = chConfigObject.ClickhouseConfig
	case protos.DBType_KAFKA:
		kaConfigObject, ok := config.(*protos.Peer_KafkaConfig)
		if !ok {
			return wrongConfigResponse, nil
		}
		innerConfig = kaConfigObject.KafkaConfig
	case protos.DBType_PUBSUB:
		psConfigObject, ok := config.(*protos.Peer_PubsubConfig)
		if !ok {
			return wrongConfigResponse, nil
		}
		innerConfig = psConfigObject.PubsubConfig
	case protos.DBType_EVENTHUBS:
		ehConfigObject, ok := config.(*protos.Peer_EventhubGroupConfig)
		if !ok {
			return wrongConfigResponse, nil
		}
		innerConfig = ehConfigObject.EventhubGroupConfig
	case protos.DBType_ELASTICSEARCH:
		esConfigObject, ok := config.(*protos.Peer_ElasticsearchConfig)
		if !ok {
			return wrongConfigResponse, nil
		}
		innerConfig = esConfigObject.ElasticsearchConfig
	case protos.DBType_ICEBERG:
		icebergConfigObject, ok := config.(*protos.Peer_IcebergConfig)
		if !ok {
			return wrongConfigResponse, nil
		}
		innerConfig = icebergConfigObject.IcebergConfig
	default:
		return wrongConfigResponse, nil
	}
	encodedConfig, encodingErr := proto.Marshal(innerConfig)
	if encodingErr != nil {
		slog.Error(fmt.Sprintf("failed to encode peer configuration for %s peer %s : %v",
			peer.Type, peer.Name, encodingErr))
		return nil, encodingErr
	}

	_, err := pool.Exec(ctx, `
		INSERT INTO peers (name, type, options)
		VALUES ($1, $2, $3)
		ON CONFLICT (name) DO UPDATE
		SET type = $2, options = $3`,
		peer.Name, peerType, encodedConfig,
	)
	if err != nil {
		return &protos.CreatePeerResponse{
			Status: protos.CreatePeerStatus_FAILED,
			Message: fmt.Sprintf("failed to upsert into peers table for %s peer %s: %s",
				peer.Type, peer.Name, err.Error()),
		}, nil
	}

	return &protos.CreatePeerResponse{
		Status:  protos.CreatePeerStatus_CREATED,
		Message: "",
	}, nil
}
