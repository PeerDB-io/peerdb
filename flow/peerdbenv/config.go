package peerdbenv

import (
	"time"
)

// This file contains functions to get the values of various peerdb environment
// variables. This will help catalog the environment variables that are used
// throughout the codebase.

// PEERDB_VERSION_SHA_SHORT
func PeerDBVersionShaShort() string {
	return getEnvString("PEERDB_VERSION_SHA_SHORT", "unknown")
}

// PEERDB_DEPLOYMENT_UID
func PeerDBDeploymentUID() string {
	return getEnvString("PEERDB_DEPLOYMENT_UID", "")
}

// PEERDB_CDC_CHANNEL_BUFFER_SIZE
func PeerDBCDCChannelBufferSize() int {
	return getEnvInt("PEERDB_CDC_CHANNEL_BUFFER_SIZE", 1<<18)
}

// PEERDB_EVENTHUB_FLUSH_TIMEOUT_SECONDS
func PeerDBEventhubFlushTimeoutSeconds() time.Duration {
	x := getEnvInt("PEERDB_EVENTHUB_FLUSH_TIMEOUT_SECONDS", 10)
	return time.Duration(x) * time.Second
}

// PEERDB_CDC_IDLE_TIMEOUT_SECONDS
func PeerDBCDCIdleTimeoutSeconds() time.Duration {
	x := getEnvInt("PEERDB_CDC_IDLE_TIMEOUT_SECONDS", 60)
	return time.Duration(x) * time.Second
}

// PEERDB_CDC_DISK_SPILL_THRESHOLD
func PeerDBCDCDiskSpillThreshold() int {
	return getEnvInt("PEERDB_CDC_DISK_SPILL_THRESHOLD", 1_000_000)
}

// PEERDB_CATALOG_HOST
func PeerDBCatalogHost() string {
	return getEnvString("PEERDB_CATALOG_HOST", "")
}

// PEERDB_CATALOG_PORT
func PeerDBCatalogPort() uint32 {
	return getEnvUint32("PEERDB_CATALOG_PORT", 5432)
}

// PEERDB_CATALOG_USER
func PeerDBCatalogUser() string {
	return getEnvString("PEERDB_CATALOG_USER", "")
}

// PEERDB_CATALOG_PASSWORD
func PeerDBCatalogPassword() string {
	return getEnvString("PEERDB_CATALOG_PASSWORD", "")
}

// PEERDB_CATALOG_DATABASE
func PeerDBCatalogDatabase() string {
	return getEnvString("PEERDB_CATALOG_DATABASE", "")
}

// PEERDB_SLOT_LAG_MB_ALERT_THRESHOLD, 0 disables slot lag alerting entirely
func PeerDBSlotLagMBAlertThreshold() uint32 {
	return getEnvUint32("PEERDB_SLOT_LAG_MB_ALERT_THRESHOLD", 5000)
}

// PEERDB_ALERTING_GAP_MINUTES, 0 disables all alerting entirely
func PeerDBAlertingGapMinutesAsDuration() time.Duration {
	why := int64(getEnvUint32("PEERDB_ALERTING_GAP_MINUTES", 15))
	return time.Duration(why) * time.Minute
}

// PEERDB_PGPEER_OPEN_CONNECTIONS_ALERT_THRESHOLD, 0 disables open connections alerting entirely
func PeerDBOpenConnectionsAlertThreshold() uint32 {
	return getEnvUint32("PEERDB_PGPEER_OPEN_CONNECTIONS_ALERT_THRESHOLD", 5)
}

// PEERDB_ENABLE_WAL_HEARTBEAT
func PeerDBEnableWALHeartbeat() bool {
	return getEnvBool("PEERDB_ENABLE_WAL_HEARTBEAT", false)
}
