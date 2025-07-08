package mongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

const MinSupportedVersion = "5.1.0"

type BuildInfo struct {
	Version string `bson:"version"`
}

type ReplSetGetStatus struct {
	Set     string `bson:"set"`
	MyState int    `bson:"myState"`
}

func GetBuildInfo(ctx context.Context, client *mongo.Client) (*BuildInfo, error) {
	singleResult := client.Database("admin").RunCommand(ctx, bson.D{bson.E{Key: "buildInfo", Value: 1}})
	if singleResult.Err() != nil {
		return nil, fmt.Errorf("failed to run 'buildInfo' command: %w", singleResult.Err())
	}
	var info BuildInfo
	if err := singleResult.Decode(&info); err != nil {
		return nil, fmt.Errorf("failed to decode BuildInfo: %w", err)
	}
	return &info, nil
}

func GetReplSetGetStatus(ctx context.Context, client *mongo.Client) (*ReplSetGetStatus, error) {
	singleResult := client.Database("admin").RunCommand(ctx, bson.D{
		bson.E{Key: "replSetGetStatus", Value: 1},
	})
	if singleResult.Err() != nil {
		return nil, fmt.Errorf("failed to run 'replSetGetStatus' command: %w", singleResult.Err())
	}
	var status ReplSetGetStatus
	if err := singleResult.Decode(&status); err != nil {
		return nil, fmt.Errorf("failed to decode ReplSetGetStatus: %w", err)
	}
	return &status, nil
}
