package mongo

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/pkg/testutil"
)

func init() {
	testutil.LoadEnv()
}

func connectMongo(t *testing.T, creds testutil.MongoTestCredentials, readPreference string) *mongo.Client {
	t.Helper()

	clientOptions, err := BuildClientOptions(ClientConfig{
		Uri:            creds.URI,
		Username:       creds.Username,
		Password:       creds.Password,
		ReadPreference: readPreference,
		DisableTls:     true,
	})
	require.NoError(t, err)

	client, err := mongo.Connect(clientOptions)
	require.NoError(t, err)
	t.Cleanup(func() {
		disconnectCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, client.Disconnect(disconnectCtx))
	})
	return client
}

// oplog retention is replica-set-wide state, so this test must not run in parallel with
// anything else that validates a mongo mirror
func TestValidateOplogRetention(t *testing.T) {
	ctx := t.Context()

	adminClient := connectMongo(t, testutil.MongoAdminTestCredentials(t), ReadPreferencePrimary)
	userClient := connectMongo(t, testutil.MongoUserTestCredentials(t), "")

	initialStatus, err := GetServerStatus(ctx, adminClient)
	require.NoError(t, err)
	t.Cleanup(func() {
		restoreCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, adminClient.Database("admin").RunCommand(restoreCtx, bson.D{
			bson.E{Key: "replSetResizeOplog", Value: 1},
			bson.E{Key: "minRetentionHours", Value: initialStatus.OplogTruncation.OplogMinRetentionHours},
		}).Err())
	})

	// test retention hours (< 24 hours) validation failure
	require.NoError(t, adminClient.Database("admin").RunCommand(ctx, bson.D{
		bson.E{Key: "replSetResizeOplog", Value: 1},
		bson.E{Key: "minRetentionHours", Value: MinOplogRetentionHours - 1},
	}).Err())
	require.ErrorContains(t, ValidateOplogRetention(ctx, userClient), "oplog retention must be set to >= 24 hours")

	// test retention hours (>= 24 hours) validation success
	require.NoError(t, adminClient.Database("admin").RunCommand(ctx, bson.D{
		bson.E{Key: "replSetResizeOplog", Value: 1},
		bson.E{Key: "minRetentionHours", Value: MinOplogRetentionHours},
	}).Err())
	require.NoError(t, ValidateOplogRetention(ctx, userClient))
}

func TestValidateUserRoles(t *testing.T) {
	adminCreds := testutil.MongoAdminTestCredentials(t)
	adminClient := connectMongo(t, adminCreds, ReadPreferencePrimary)

	tests := []struct {
		name    string
		wantErr string
		roles   bson.A
	}{
		{
			name:    "no roles",
			roles:   bson.A{},
			wantErr: "missing required role: readAnyDatabase",
		},
		{
			name:    "readAnyDatabase only",
			roles:   bson.A{"readAnyDatabase"},
			wantErr: "missing required role: clusterMonitor",
		},
		{
			name:    "clusterMonitor only",
			roles:   bson.A{"clusterMonitor"},
			wantErr: "missing required role: readAnyDatabase",
		},
		{
			name:  "both required roles",
			roles: bson.A{"readAnyDatabase", "clusterMonitor"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()

			// a user per case, so the cases stay independent and a failure cannot
			// strand a user that the next run would collide with
			user := "pkgmongo_" + strings.ToLower(common.RandomString(8))
			password := common.RandomString(16)
			require.NoError(t, adminClient.Database("admin").RunCommand(ctx, bson.D{
				bson.E{Key: "createUser", Value: user},
				bson.E{Key: "pwd", Value: password},
				bson.E{Key: "roles", Value: tt.roles},
			}).Err())
			t.Cleanup(func() {
				dropCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				require.NoError(t, adminClient.Database("admin").RunCommand(dropCtx, bson.D{
					bson.E{Key: "dropUser", Value: user},
				}).Err())
			})

			userClient := connectMongo(t, testutil.MongoTestCredentials{
				URI:      adminCreds.URI,
				Username: user,
				Password: password,
			}, "")

			err := ValidateUserRoles(ctx, userClient)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
