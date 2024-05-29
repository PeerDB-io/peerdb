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
	wrongConfigResponse := &protos.CreatePeerResponse{
		Status: protos.CreatePeerStatus_FAILED,
		Message: fmt.Sprintf("invalid config for %s peer %s",
			peer.Type, peer.Name),
	}
	var encodedConfig []byte
	var encodingErr error
	peerType := peer.Type
	switch peerType {
	case protos.DBType_POSTGRES:
		pgConfigObject, ok := config.(*protos.Peer_PostgresConfig)
		if !ok {
			return wrongConfigResponse, nil
		}
		pgConfig := pgConfigObject.PostgresConfig
		encodedConfig, encodingErr = proto.Marshal(pgConfig)
	case protos.DBType_SNOWFLAKE:
		sfConfigObject, ok := config.(*protos.Peer_SnowflakeConfig)
		if !ok {
			return wrongConfigResponse, nil
		}
		sfConfig := sfConfigObject.SnowflakeConfig
		encodedConfig, encodingErr = proto.Marshal(sfConfig)
	case protos.DBType_BIGQUERY:
		bqConfigObject, ok := config.(*protos.Peer_BigqueryConfig)
		if !ok {
			return wrongConfigResponse, nil
		}
		bqConfig := bqConfigObject.BigqueryConfig
		encodedConfig, encodingErr = proto.Marshal(bqConfig)
	case protos.DBType_SQLSERVER:
		sqlServerConfigObject, ok := config.(*protos.Peer_SqlserverConfig)
		if !ok {
			return wrongConfigResponse, nil
		}
		sqlServerConfig := sqlServerConfigObject.SqlserverConfig
		encodedConfig, encodingErr = proto.Marshal(sqlServerConfig)
	case protos.DBType_S3:
		s3ConfigObject, ok := config.(*protos.Peer_S3Config)
		if !ok {
			return wrongConfigResponse, nil
		}
		s3Config := s3ConfigObject.S3Config
		encodedConfig, encodingErr = proto.Marshal(s3Config)
	case protos.DBType_CLICKHOUSE:
		chConfigObject, ok := config.(*protos.Peer_ClickhouseConfig)
		if !ok {
			return wrongConfigResponse, nil
		}
		chConfig := chConfigObject.ClickhouseConfig
		encodedConfig, encodingErr = proto.Marshal(chConfig)
	case protos.DBType_KAFKA:
		kaConfigObject, ok := config.(*protos.Peer_KafkaConfig)
		if !ok {
			return wrongConfigResponse, nil
		}
		kaConfig := kaConfigObject.KafkaConfig
		encodedConfig, encodingErr = proto.Marshal(kaConfig)
	case protos.DBType_PUBSUB:
		psConfigObject, ok := config.(*protos.Peer_PubsubConfig)
		if !ok {
			return wrongConfigResponse, nil
		}
		psConfig := psConfigObject.PubsubConfig
		encodedConfig, encodingErr = proto.Marshal(psConfig)
	case protos.DBType_EVENTHUBS:
		ehConfigObject, ok := config.(*protos.Peer_EventhubGroupConfig)
		if !ok {
			return wrongConfigResponse, nil
		}
		ehConfig := ehConfigObject.EventhubGroupConfig
		encodedConfig, encodingErr = proto.Marshal(ehConfig)
	case protos.DBType_ELASTICSEARCH:
		esConfigObject, ok := config.(*protos.Peer_ElasticsearchConfig)
		if !ok {
			return wrongConfigResponse, nil
		}
		esConfig := esConfigObject.ElasticsearchConfig
		encodedConfig, encodingErr = proto.Marshal(esConfig)
	default:
		return wrongConfigResponse, nil
	}
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
