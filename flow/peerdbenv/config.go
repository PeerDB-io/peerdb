package peerdbenv

import (
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/shared"
)

// This file contains functions to get the values of various peerdb environment
// variables. This will help catalog the environment variables that are used
// throughout the codebase.

// PEERDB_VERSION_SHA_SHORT
func PeerDBVersionShaShort() string {
	return GetEnvString("PEERDB_VERSION_SHA_SHORT", "unknown")
}

// PEERDB_DEPLOYMENT_UID
func PeerDBDeploymentUID() string {
	return GetEnvString("PEERDB_DEPLOYMENT_UID", "")
}

func PeerFlowTaskQueueName(taskQueueID shared.TaskQueueID) string {
	deploymentUID := PeerDBDeploymentUID()
	if deploymentUID == "" {
		return string(taskQueueID)
	}
	return fmt.Sprintf("%s-%s", deploymentUID, taskQueueID)
}

// PEERDB_CDC_CHANNEL_BUFFER_SIZE
func PeerDBCDCChannelBufferSize() int {
	return getEnvInt("PEERDB_CDC_CHANNEL_BUFFER_SIZE", 1<<18)
}

// PEERDB_QUEUE_FLUSH_TIMEOUT_SECONDS
func PeerDBQueueFlushTimeoutSeconds() time.Duration {
	x := getEnvInt("PEERDB_QUEUE_FLUSH_TIMEOUT_SECONDS", 10)
	return time.Duration(x) * time.Second
}

func PeerDBQueueParallelism() int {
	return getEnvInt("PEERDB_QUEUE_PARALLELISM", 4)
}

// env variable doesn't exist anymore, but tests appear to depend on this
// in lieu of an actual value of IdleTimeoutSeconds
func PeerDBCDCIdleTimeoutSeconds(providedValue int) time.Duration {
	var x int
	if providedValue > 0 {
		x = providedValue
	} else {
		x = getEnvInt("", 10)
	}
	return time.Duration(x) * time.Second
}

// PEERDB_CDC_DISK_SPILL_RECORDS_THRESHOLD
func PeerDBCDCDiskSpillRecordsThreshold() int {
	return getEnvInt("PEERDB_CDC_DISK_SPILL_RECORDS_THRESHOLD", 1_000_000)
}

// PEERDB_CDC_DISK_SPILL_RECORDS_THRESHOLD, negative numbers means memory threshold disabled
func PeerDBCDCDiskSpillMemPercentThreshold() int {
	return getEnvInt("PEERDB_CDC_DISK_SPILL_MEM_PERCENT_THRESHOLD", -1)
}

// GOMEMLIMIT is a variable internal to Golang itself, we use this for internal targets, 0 means no maximum
func PeerDBFlowWorkerMaxMemBytes() uint64 {
	return getEnvUint[uint64]("GOMEMLIMIT", 0)
}

// PEERDB_CATALOG_HOST
func PeerDBCatalogHost() string {
	return GetEnvString("PEERDB_CATALOG_HOST", "")
}

// PEERDB_CATALOG_PORT
func PeerDBCatalogPort() uint16 {
	return getEnvUint[uint16]("PEERDB_CATALOG_PORT", 5432)
}

// PEERDB_CATALOG_USER
func PeerDBCatalogUser() string {
	return GetEnvString("PEERDB_CATALOG_USER", "")
}

// PEERDB_CATALOG_PASSWORD
func PeerDBCatalogPassword() string {
	return GetEnvString("PEERDB_CATALOG_PASSWORD", "")
}

// PEERDB_CATALOG_DATABASE
func PeerDBCatalogDatabase() string {
	return GetEnvString("PEERDB_CATALOG_DATABASE", "")
}

// PEERDB_ENABLE_WAL_HEARTBEAT
func PeerDBEnableWALHeartbeat() bool {
	return getEnvBool("PEERDB_ENABLE_WAL_HEARTBEAT", false)
}

// PEERDB_WAL_HEARTBEAT_QUERY
func PeerDBWALHeartbeatQuery() string {
	return GetEnvString("PEERDB_WAL_HEARTBEAT_QUERY", `BEGIN;
DROP AGGREGATE IF EXISTS PEERDB_EPHEMERAL_HEARTBEAT(float4);
CREATE AGGREGATE PEERDB_EPHEMERAL_HEARTBEAT(float4) (SFUNC = float4pl, STYPE = float4);
DROP AGGREGATE PEERDB_EPHEMERAL_HEARTBEAT(float4);
END;`)
}

// PEERDB_ENABLE_PARALLEL_SYNC_NORMALIZE
func PeerDBEnableParallelSyncNormalize() bool {
	return getEnvBool("PEERDB_ENABLE_PARALLEL_SYNC_NORMALIZE", false)
}

func PeerDBSnowflakeMergeParallelism() int {
	return getEnvInt("PEERDB_SNOWFLAKE_MERGE_PARALLELISM", 8)
}

// PEERDB_TELEMETRY_AWS_SNS_TOPIC_ARN
func PeerDBTelemetryAWSSNSTopicArn() string {
	return GetEnvString("PEERDB_TELEMETRY_AWS_SNS_TOPIC_ARN", "")
}

func PeerDBAlertingEmailSenderSourceEmail() string {
	return GetEnvString("PEERDB_ALERTING_EMAIL_SENDER_SOURCE_EMAIL", "")
}

func PeerDBAlertingEmailSenderConfigurationSet() string {
	return GetEnvString("PEERDB_ALERTING_EMAIL_SENDER_CONFIGURATION_SET", "")
}

func PeerDBAlertingEmailSenderRegion() string {
	return GetEnvString("PEERDB_ALERTING_EMAIL_SENDER_REGION", "")
}

// Comma-separated reply-to addresses
func PeerDBAlertingEmailSenderReplyToAddresses() string {
	return GetEnvString("PEERDB_ALERTING_EMAIL_SENDER_REPLY_TO_ADDRESSES", "")
}

// PEERDB_CLICKHOUSE_AWS_S3_BUCKET_NAME
func PeerDBClickhouseAWSS3BucketName() string {
	return GetEnvString("PEERDB_CLICKHOUSE_AWS_S3_BUCKET_NAME", "")
}
