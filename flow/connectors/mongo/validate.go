package connmongo

import (
	"context"
	"errors"
	"fmt"

	"github.com/Masterminds/semver"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func (c *MongoConnector) ValidateCheck(ctx context.Context) error {
	mongoVersion, err := c.GetVersion(ctx)
	if err != nil {
		return err
	}
	parsedVersion, err := semver.NewVersion(mongoVersion)
	if err != nil {
		return fmt.Errorf("failed to parse mongo version %s: %w", mongoVersion, err)
	}
	minSupportedVersion := semver.MustParse("5.1.0")
	if parsedVersion.Compare(minSupportedVersion) == -1 {
		return fmt.Errorf("require minimum mongo version %s, got %s", minSupportedVersion, mongoVersion)
	}
	return nil
}

func (c *MongoConnector) ValidateMirrorSource(ctx context.Context, cfg *protos.FlowConnectionConfigs) error {
	if cfg.DoInitialSnapshot && cfg.InitialSnapshotOnly {
		return nil
	}

	// Check if MongoDB is configured as a replica set
	var result bson.M
	if err := c.client.Database("admin").RunCommand(ctx, bson.D{
		{Key: "replSetGetStatus", Value: 1},
	}).Decode(&result); err != nil {
		return fmt.Errorf("failed to get replica set status: %w", err)
	}

	// Check if this is a replica set
	if _, ok := result["set"]; !ok {
		return errors.New("MongoDB is not configured as a replica set, which is required for CDC")
	}

	if myState, ok := result["myState"]; !ok {
		return errors.New("myState not found in response")
	} else if myStateInt, ok := myState.(int32); !ok {
		return fmt.Errorf("failed to convert myState %v to int32", myState)
	} else if myStateInt != 1 {
		return fmt.Errorf("MongoDB is not the primary node in the replica set, current state: %d", myState)
	}

	return nil
}
