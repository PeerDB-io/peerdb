package model

import (
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type StreamConsistencyStats struct {
	StatsByTable map[string]*CdcTableConsistencyStats
}

func NewStreamConsistencyStats() *StreamConsistencyStats {
	return &StreamConsistencyStats{
		StatsByTable: map[string]*CdcTableConsistencyStats{},
	}
}

func (ss *StreamConsistencyStats) Get(tableName string) *CdcTableConsistencyStats {
	if ss == nil {
		return nil
	}
	stats, ok := ss.StatsByTable[tableName]
	if !ok {
		stats = NewCdcTableConsistencyStats()
		ss.StatsByTable[tableName] = stats
	}
	return stats
}

func (ss *StreamConsistencyStats) Log(logger log.Logger) {
	for tableName, tableStats := range ss.StatsByTable {
		tableStats.Log(tableName, logger)
	}
}

type CdcTableConsistencyStats struct {
	StatsByColumn map[string]*qvalue.ConsistencyStat
}

func NewCdcTableConsistencyStats() *CdcTableConsistencyStats {
	return &CdcTableConsistencyStats{
		StatsByColumn: map[string]*qvalue.ConsistencyStat{},
	}
}

func (ts *CdcTableConsistencyStats) Get(columnName string) *qvalue.ConsistencyStat {
	if ts == nil {
		return nil
	}
	stat, ok := ts.StatsByColumn[columnName]
	if !ok {
		stat = &qvalue.ConsistencyStat{}
		ts.StatsByColumn[columnName] = stat
	}
	return stat
}

func (ts *CdcTableConsistencyStats) Log(tableName string, logger log.Logger) {
	for columnName, stat := range ts.StatsByColumn {
		qvalue.LogConsistencyStat(stat, tableName, columnName, logger)
	}
}

type SnapshotTableConsistencyStats struct {
	stats  []qvalue.ConsistencyStat
	fields []types.QField
}

func NewSnapshotTableConsistencyStats(fields []types.QField) *SnapshotTableConsistencyStats {
	return &SnapshotTableConsistencyStats{
		stats:  make([]qvalue.ConsistencyStat, len(fields)),
		fields: fields,
	}
}

func (ts *SnapshotTableConsistencyStats) Get(idx int) *qvalue.ConsistencyStat {
	if ts == nil {
		return nil
	}
	return &ts.stats[idx]
}

func (ts *SnapshotTableConsistencyStats) Log(tableName string, logger log.Logger) {
	if ts == nil {
		return
	}
	for i, field := range ts.fields {
		qvalue.LogConsistencyStat(&ts.stats[i], tableName, field.Name, logger)
	}
}
