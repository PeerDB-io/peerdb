package mongo

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

const (
	MinSupportedVersion    = "4.4.0"
	MinOplogRetentionHours = 24

	ReplicaSet     = "ReplicaSet"
	ShardedCluster = "ShardedCluster"
)

var RequiredRoles = [...]string{"readAnyDatabase", "clusterMonitor"}

func ValidateServerCompatibility(ctx context.Context, client *mongo.Client) error {
	buildInfo, err := GetBuildInfo(ctx, client)
	if err != nil {
		return err
	}

	if cmp, err := CompareServerVersions(buildInfo.Version, MinSupportedVersion); err != nil {
		return err
	} else if cmp < 0 {
		return fmt.Errorf("require minimum mongo version %s", MinSupportedVersion)
	}

	validateStorageEngine := func(instanceCtx context.Context, instanceClient *mongo.Client) error {
		ss, err := GetServerStatus(instanceCtx, instanceClient)
		if err != nil {
			return err
		}

		if ss.StorageEngine.Name != "wiredTiger" {
			return errors.New("only wiredTiger storage engine is supported")
		}
		return nil
	}

	topologyType, err := GetTopologyType(ctx, client)
	if err != nil {
		return err
	}

	if topologyType == ReplicaSet {
		return validateStorageEngine(ctx, client)
	} else {
		// TODO: run validation on shard
		return nil
	}
}

func ValidateUserRoles(ctx context.Context, client *mongo.Client) error {
	connectionStatus, err := GetConnectionStatus(ctx, client)
	if err != nil {
		return err
	}

	for _, requiredRole := range RequiredRoles {
		if !slices.ContainsFunc(connectionStatus.AuthInfo.AuthenticatedUserRoles, func(r Role) bool {
			return r.Role == requiredRole
		}) {
			return fmt.Errorf("missing required role: %s", requiredRole)
		}
	}

	return nil
}

func ValidateOplogRetention(ctx context.Context, client *mongo.Client) error {
	validateOplogRetention := func(instanceCtx context.Context, instanceClient *mongo.Client) error {
		ss, err := GetServerStatus(instanceCtx, instanceClient)
		if err != nil {
			return err
		}
		if ss.OplogTruncation.OplogMinRetentionHours == 0 ||
			ss.OplogTruncation.OplogMinRetentionHours < MinOplogRetentionHours {
			return fmt.Errorf("oplog retention must be set to >= 24 hours, but got %f",
				ss.OplogTruncation.OplogMinRetentionHours)
		}
		return nil
	}

	topology, err := GetTopologyType(ctx, client)
	if err != nil {
		return err
	}
	if topology == ReplicaSet {
		return validateOplogRetention(ctx, client)
	} else {
		// TODO: run validation on shard
		return nil
	}
}

func GetTopologyType(ctx context.Context, client *mongo.Client) (string, error) {
	hello, err := GetHelloResponse(ctx, client)
	if err != nil {
		return "", err
	}

	// Only replica set has 'hosts' field
	// https://www.mongodb.com/docs/manual/reference/command/hello/#mongodb-data-hello.hosts
	if len(hello.Hosts) > 0 {
		return ReplicaSet, nil
	}

	// Only sharded cluster has 'msg' field, and equals to 'isdbgrid'
	// https://www.mongodb.com/docs/manual/reference/command/hello/#mongodb-data-hello.msg
	if hello.Msg == "isdbgrid" {
		return ShardedCluster, nil
	}
	return "", errors.New("topology type must be ReplicaSet or ShardedCluster")
}
