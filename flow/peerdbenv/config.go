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
	return getEnvString("PEERDB_CATALOG_HOST", "")
}

// PEERDB_CATALOG_PORT
func PeerDBCatalogPort() uint16 {
	return getEnvUint[uint16]("PEERDB_CATALOG_PORT", 5432)
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

// PEERDB_ENABLE_WAL_HEARTBEAT
func PeerDBEnableWALHeartbeat() bool {
	return getEnvBool("PEERDB_ENABLE_WAL_HEARTBEAT", false)
}

// PEERDB_ENABLE_PARALLEL_SYNC_NORMALIZE
func PeerDBEnableParallelSyncNormalize() bool {
	return getEnvBool("PEERDB_ENABLE_PARALLEL_SYNC_NORMALIZE", false)
}
