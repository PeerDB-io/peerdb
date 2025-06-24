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
		statsByTable[tableMapping.DestinationTableIdentifier] = NewCdcTableNumericTruncator(
			tableMapping.DestinationTableIdentifier, tableMapping.Columns, typesToSkip)
	}
	return &StreamNumericTruncator{
		TruncatorsByTable: statsByTable,
	}
}

func (ss *StreamNumericTruncator) Get(destinationTable string) *CdcTableNumericTruncator {
	if ss == nil {
		return nil
	}
	truncator, ok := ss.TruncatorsByTable[destinationTable]
	if !ok {
		truncator = NewCdcTableNumericTruncator(destinationTable, nil, nil)
		ss.TruncatorsByTable[destinationTable] = truncator
	}
	return truncator
}

func (ss *StreamNumericTruncator) Warnings() []error {
	var warnings []error
	for _, tableStats := range ss.TruncatorsByTable {
		tableStats.CollectWarnings(&warnings)
	}
	return warnings
}

type CdcTableNumericTruncator struct {
	DestinationTable   string
	TruncatorsByColumn map[string]*CdcColumnNumericTruncator
}

func NewCdcTableNumericTruncator(
	destinationTable string, columnSettings []*protos.ColumnSetting, typesToSkip map[string]struct{},
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
		DestinationTable:   destinationTable,
		TruncatorsByColumn: truncatorsByColumn,
	}
}

func (ts *CdcTableNumericTruncator) Get(destinationColumn string) *CdcColumnNumericTruncator {
	if ts == nil {
		return &CdcColumnNumericTruncator{Skip: true}
	}
	stat, ok := ts.TruncatorsByColumn[destinationColumn]
	if !ok {
		stat = &CdcColumnNumericTruncator{
			Stat: qvalue.NewNumericStat(ts.DestinationTable, destinationColumn),
		}
		ts.TruncatorsByColumn[destinationColumn] = stat
	}
	return stat
}

func (ts *CdcTableNumericTruncator) CollectWarnings(warnings *[]error) {
	for _, truncator := range ts.TruncatorsByColumn {
		if !truncator.Skip {
			truncator.Stat.CollectWarnings(warnings)
		}
	}
}

//nolint:govet // semantically ordered
type CdcColumnNumericTruncator struct {
	Skip bool
	Stat *qvalue.NumericStat
}

type SnapshotTableNumericTruncator struct {
	stats []*qvalue.NumericStat
}

func NewSnapshotTableNumericTruncator(destinationTable string, fields []types.QField) *SnapshotTableNumericTruncator {
	stats := make([]*qvalue.NumericStat, 0, len(fields))
	for _, field := range fields {
		stats = append(stats, qvalue.NewNumericStat(destinationTable, field.Name))
	}
	return &SnapshotTableNumericTruncator{
		stats: stats,
	}
}

func (ts *SnapshotTableNumericTruncator) Get(idx int) *qvalue.NumericStat {
	if ts == nil {
		return nil
	}
	return ts.stats[idx]
}

func (ts *SnapshotTableNumericTruncator) Warnings() []error {
	var warnings []error
	for _, stat := range ts.stats {
		stat.CollectWarnings(&warnings)
	}
	return warnings
}
