package model

import (
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type CdcTableNumericTruncator struct {
	TruncatorsByColumn map[string]CdcColumnNumericTruncator
	DestinationTable   string
}

type StreamNumericTruncator map[string]CdcTableNumericTruncator

func NewStreamNumericTruncator(tableMappings []*protos.TableMapping, typesToSkip map[string]struct{}) StreamNumericTruncator {
	statsByTable := make(map[string]CdcTableNumericTruncator, len(tableMappings))
	for _, tableMapping := range tableMappings {
		statsByTable[tableMapping.DestinationTableIdentifier] = NewCdcTableNumericTruncator(
			tableMapping.DestinationTableIdentifier, tableMapping.Columns, typesToSkip)
	}
	return statsByTable
}

func (ss StreamNumericTruncator) Get(destinationTable string) CdcTableNumericTruncator {
	if ss == nil {
		return CdcTableNumericTruncator{}
	}
	truncator, ok := ss[destinationTable]
	if !ok {
		truncator = NewCdcTableNumericTruncator(destinationTable, nil, nil)
		ss[destinationTable] = truncator
	}
	return truncator
}

func (ss StreamNumericTruncator) Warnings() shared.QRepWarnings {
	var warnings shared.QRepWarnings
	for _, tableStats := range ss {
		tableStats.CollectWarnings(&warnings)
	}
	return warnings
}

func NewCdcTableNumericTruncator(
	destinationTable string, columnSettings []*protos.ColumnSetting, typesToSkip map[string]struct{},
) CdcTableNumericTruncator {
	truncatorsByColumn := map[string]CdcColumnNumericTruncator{}
	for _, columnSetting := range columnSettings {
		if _, ok := typesToSkip[columnSetting.DestinationType]; ok {
			destinationName := columnSetting.DestinationName
			if destinationName == "" {
				destinationName = columnSetting.SourceName
			}
			truncatorsByColumn[destinationName] = CdcColumnNumericTruncator{}
		}
	}
	return CdcTableNumericTruncator{
		TruncatorsByColumn: truncatorsByColumn,
		DestinationTable:   destinationTable,
	}
}

func (ts CdcTableNumericTruncator) Get(destinationColumn string) CdcColumnNumericTruncator {
	if ts.TruncatorsByColumn == nil {
		return CdcColumnNumericTruncator{}
	}
	stat, ok := ts.TruncatorsByColumn[destinationColumn]
	if !ok {
		numericStat := qvalue.NewNumericStat(ts.DestinationTable, destinationColumn)
		stat = CdcColumnNumericTruncator{
			Stat: &numericStat,
		}
		ts.TruncatorsByColumn[destinationColumn] = stat
	}
	return stat
}

func (ts CdcTableNumericTruncator) CollectWarnings(warnings *shared.QRepWarnings) {
	for _, truncator := range ts.TruncatorsByColumn {
		if truncator.Stat != nil {
			truncator.Stat.CollectWarnings(warnings)
		}
	}
}

type CdcColumnNumericTruncator struct {
	Stat *qvalue.NumericStat
}

type SnapshotTableNumericTruncator []qvalue.NumericStat

func NewSnapshotTableNumericTruncator(destinationTable string, fields []types.QField) SnapshotTableNumericTruncator {
	stats := make([]qvalue.NumericStat, 0, len(fields))
	for _, field := range fields {
		stats = append(stats, qvalue.NewNumericStat(destinationTable, field.Name))
	}
	return SnapshotTableNumericTruncator(stats)
}

func (ts SnapshotTableNumericTruncator) Get(idx int) *qvalue.NumericStat {
	if ts == nil {
		return nil
	}
	return &ts[idx]
}

func (ts SnapshotTableNumericTruncator) Warnings() shared.QRepWarnings {
	var warnings shared.QRepWarnings
	for _, stat := range ts {
		stat.CollectWarnings(&warnings)
	}
	return warnings
}
