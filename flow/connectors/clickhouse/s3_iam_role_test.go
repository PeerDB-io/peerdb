package connclickhouse

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	avro "github.com/PeerDB-io/peerdb/flow/connectors/utils/avro"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestIAMRoleCanIssueSelectFromS3(t *testing.T) {
	for _, envVar := range []string{
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_ACCESS_KEY",
		"AWS_REGION",
		"AWS_ENDPOINT_URL_S3",
		"AWS_S3_BUCKET_NAME",
		"PEERDB_CLICKHOUSE_AWS_CREDENTIALS_AWS_ACCESS_KEY_ID",
		"PEERDB_CLICKHOUSE_AWS_CREDENTIALS_AWS_SECRET_ACCESS_KEY",
		"PEERDB_CLICKHOUSE_AWS_CREDENTIALS_AWS_REGION",
		"PEERDB_CLICKHOUSE_AWS_CREDENTIALS_AWS_ENDPOINT_URL_S3",
		"PEERDB_CLICKHOUSE_AWS_S3_BUCKET_NAME",
	} {
		t.Setenv(envVar, "")
	}
	for _, envVar := range []string{
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_ACCESS_KEY",
		"AWS_SESSION_TOKEN",
	} {
		t.Setenv(envVar, os.Getenv("FLOW_TESTS_"+envVar))
	}
	t.Setenv("PEERDB_CLICKHOUSE_AWS_S3_BUCKET_NAME", os.Getenv("FLOW_TESTS_AWS_S3_BUCKET_NAME"))
	ctx := t.Context()

	conn, err := NewClickHouseConnector(ctx, nil,
		&protos.ClickhouseConfig{
			Host:       "localhost",
			Port:       9000,
			Database:   "default",
			DisableTls: true,
		})
	require.NoError(t, err)
	defer conn.Close()

	flowName := fmt.Sprintf("test_iam_role_can_issue_select_from_s3_%v", time.Now().Unix())
	table, err := conn.CreateRawTable(ctx, &protos.CreateRawTableInput{
		FlowJobName: flowName,
	})
	require.NoError(t, err)
	avroSync := NewClickHouseAvroSyncMethod(&protos.QRepConfig{
		DestinationTableIdentifier: table.TableIdentifier,
	}, conn)

	require.NoError(t, err)
	err = avroSync.CopyStageToDestination(ctx, &avro.AvroFile{
		FilePath:        "test-iam-role-can-issue-select-from-s3/datafile.avro.zst",
		StorageLocation: avro.AvroS3Storage,
		NumRecords:      3,
	})
	require.NoError(t, err)

	query, err := conn.query(ctx, `SELECT COUNT(*) FROM default.`+table.TableIdentifier)
	require.NoError(t, err)
	for query.Next() {
		var count uint64
		err = query.Scan(&count)
		require.NoError(t, err)
		require.Equal(t, uint64(3), count)
	}
}
