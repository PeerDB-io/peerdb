package model

import (
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type StreamNumericTruncator struct {
	TruncatorsByTable map[string]*CdcTableNumericTruncator
}

func NewStreamNumericTruncator(tableMappings []*protos.TableMapping, typesToSkip map[string]struct{}) *StreamNumericTruncator {
	statsByTable := make(map[string]*CdcTableNumericTruncator, len(tableMappings))
	for _, tableMapping := range tableMappings {
		statsByTable[tableMapping.DestinationTableIdentifier] = NewCdcTableNumericTruncator(tableMapping.Columns, typesToSkip)
	}
	return &StreamNumericTruncator{
		TruncatorsByTable: statsByTable,
	}
}

func (ss *StreamNumericTruncator) Get(tableName string) *CdcTableNumericTruncator {
	if ss == nil {
		return nil
	}
	truncator, ok := ss.TruncatorsByTable[tableName]
	if !ok {
		truncator = NewCdcTableNumericTruncator(nil, nil)
		ss.TruncatorsByTable[tableName] = truncator
	}
	return truncator
}

func (ss *StreamNumericTruncator) Messages() []string {
	var messages []string
	for tableName, tableStats := range ss.TruncatorsByTable {
		tableStats.CollectMessages(tableName, &messages)
	}
	return messages
}

type CdcTableNumericTruncator struct {
	TruncatorsByColumn map[string]*CdcColumnNumericTruncator
}

func NewCdcTableNumericTruncator(
	columnSettings []*protos.ColumnSetting, typesToSkip map[string]struct{},
) *CdcTableNumericTruncator {
	truncatorsByColumn := map[string]*CdcColumnNumericTruncator{}
	for _, columnSetting := range columnSettings {
		if _, ok := typesToSkip[columnSetting.DestinationType]; ok {
			destinationName := columnSetting.DestinationName
			if destinationName == "" {
				destinationName = columnSetting.SourceName
			}
			truncatorsByColumn[destinationName] = &CdcColumnNumericTruncator{Skip: true}
		}
	}
	return &CdcTableNumericTruncator{
		TruncatorsByColumn: truncatorsByColumn,
	}
}

func (ts *CdcTableNumericTruncator) Get(columnName string) *CdcColumnNumericTruncator {
	if ts == nil {
		return &CdcColumnNumericTruncator{Skip: true}
	}
	stat, ok := ts.TruncatorsByColumn[columnName]
	if !ok {
		stat = &CdcColumnNumericTruncator{
			Stat: &qvalue.NumericStat{},
		}
		ts.TruncatorsByColumn[columnName] = stat
	}
	return stat
}

func (ts *CdcTableNumericTruncator) CollectMessages(tableName string, messages *[]string) {
	for columnName, truncator := range ts.TruncatorsByColumn {
		if !truncator.Skip {
			qvalue.NumericStatCollectMessages(truncator.Stat, tableName, columnName, messages)
		}
	}
}

//nolint:govet // semantically ordered
type CdcColumnNumericTruncator struct {
	Skip bool
	Stat *qvalue.NumericStat
}

type SnapshotTableNumericTruncator struct {
	stats  []qvalue.NumericStat
	fields []types.QField
}

func NewSnapshotTableNumericTruncator(fields []types.QField) *SnapshotTableNumericTruncator {
	return &SnapshotTableNumericTruncator{
		stats:  make([]qvalue.NumericStat, len(fields)),
		fields: fields,
	}
}

func (ts *SnapshotTableNumericTruncator) Get(idx int) *qvalue.NumericStat {
	if ts == nil {
		return nil
	}
	return &ts.stats[idx]
}

func (ts *SnapshotTableNumericTruncator) Messages(tableName string) []string {
	if ts == nil {
		return nil
	}
	var messages []string
	for i, field := range ts.fields {
		qvalue.NumericStatCollectMessages(&ts.stats[i], tableName, field.Name, &messages)
	}
	return messages
}
