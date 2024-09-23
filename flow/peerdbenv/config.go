package peerdbenv

import (
	"fmt"
	"log/slog"
	"strings"
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
	val, err := GetKMSDecryptedEnvString("PEERDB_CATALOG_PASSWORD", "")
	if err != nil {
		slog.Error("failed to decrypt PEERDB_CATALOG_PASSWORD", "error", err)
		panic(err)
	}

	return val
}

// PEERDB_CATALOG_DATABASE
func PeerDBCatalogDatabase() string {
	return GetEnvString("PEERDB_CATALOG_DATABASE", "")
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

func PeerDBCurrentEncKeyID() string {
	return GetEnvString("PEERDB_CURRENT_ENC_KEY_ID", "")
}

func PeerDBEncKeys() shared.PeerDBEncKeys {
	return GetEnvJSON[shared.PeerDBEncKeys]("PEERDB_ENC_KEYS", nil)
}

func PeerDBCurrentEncKey() (shared.PeerDBEncKey, error) {
	encKeyID := PeerDBCurrentEncKeyID()
	encKeys := PeerDBEncKeys()
	return encKeys.Get(encKeyID)
}

func PeerDBAllowedTargets() string {
	return GetEnvString("PEERDB_ALLOWED_TARGETS", "")
}

func PeerDBClickHouseAllowedDomains() string {
	return GetEnvString("PEERDB_CLICKHOUSE_ALLOWED_DOMAINS", "")
}

func PeerDBTemporalEnableCertAuth() bool {
	cert := GetEnvString("TEMPORAL_CLIENT_CERT", "")
	return strings.TrimSpace(cert) != ""
}

func PeerDBTemporalClientCert() ([]byte, error) {
	return GetEnvBase64EncodedBytes("TEMPORAL_CLIENT_CERT", nil)
}

func PeerDBTemporalClientKey() ([]byte, error) {
	return GetEnvBase64EncodedBytes("TEMPORAL_CLIENT_KEY", nil)
}
