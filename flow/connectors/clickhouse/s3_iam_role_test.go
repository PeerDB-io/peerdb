package connclickhouse

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestIAMRoleCanIssueSelectFromS3(t *testing.T) {
	const bucketNameEnvVar = "FLOW_TESTS_AWS_S3_BUCKET_NAME"
	if os.Getenv(bucketNameEnvVar) == "" {
		t.Skipf("skipping test since %s is not set", bucketNameEnvVar)
	}

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
	t.Setenv("PEERDB_CLICKHOUSE_AWS_S3_BUCKET_NAME", os.Getenv(bucketNameEnvVar))
	ctx := t.Context()

	chHost := "localhost"
	if host := os.Getenv("CI_CLICKHOUSE_HOST"); host != "" {
		chHost = host
	}
	var chPort uint32 = 9000
	if port := os.Getenv("CI_CLICKHOUSE_NATIVE_PORT"); port != "" {
		if p, err := strconv.ParseUint(port, 10, 32); err == nil {
			chPort = uint32(p)
		}
	}

	conn, err := NewClickHouseConnector(ctx, nil,
		&protos.ClickhouseConfig{
			Host:       chHost,
			Port:       chPort,
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
	require.NoError(t, avroSync.CopyStageToDestination(ctx, utils.AvroFile{
		FilePath:        "test-iam-role-can-issue-select-from-s3/datafile.avro.zst",
		StorageLocation: utils.AvroS3Storage,
		NumRecords:      3,
	}))

	query, err := conn.query(ctx, `SELECT COUNT(*) FROM default.`+table.TableIdentifier)
	require.NoError(t, err)
	for query.Next() {
		var count uint64
		require.NoError(t, query.Scan(&count))
		require.Equal(t, uint64(3), count)
	}
}
