package utils

import (
	"hash/fnv"
	"strconv"

	"github.com/PeerDB-io/peerdb/flow/shared"
)

func hashString(s string) uint32 {
	h := fnv.New32a()
	h.Write(shared.UnsafeFastStringToReadOnlyBytes(s))
	return h.Sum32()
}

func HashedPartitionKey(s string, numPartitions uint32) string {
	hashValue := hashString(s)
	partition := hashValue % numPartitions
	return strconv.FormatUint(uint64(partition), 10)
}
