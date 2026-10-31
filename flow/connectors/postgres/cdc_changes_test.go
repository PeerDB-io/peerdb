package connpostgres

import (
	"log/slog"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/model"
)

func TestProcessUpdateMessageKeepsUnchangedToastOnKeyChange(t *testing.T) {
	t.Parallel()
	p := &PostgresCDCSource{
		PostgresConnector:     &PostgresConnector{logger: log.NewStructuredLogger(slog.Default())},
		srcTableIDNameMapping: map[uint32]string{1: "s.t"},
		tableNameMapping:      map[string]model.SourceTableMapping{"s.t": {Name: "d.t"}},
		relationMessageMapping: model.RelationMessageMapping{1: {RelationID: 1, Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 23, Flags: 1}, {Name: "big", DataType: 25}}}},
	}
	update := func(oldType uint8, old *pglogrepl.TupleDataColumn) *model.UpdateRecord[model.PgItems] {
		rec, err := processUpdateMessage(p, 0, &pglogrepl.UpdateMessage{RelationID: 1, OldTupleType: oldType,
			OldTuple: &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{{DataType: 't', Data: []byte("1")}, old}},
			NewTuple: &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{{DataType: 't', Data: []byte("2")}, {DataType: 'u'}}},
		}, pgProcessor{}, nil, map[string]struct{}{})
		require.NoError(t, err)
		return rec.(*model.UpdateRecord[model.PgItems])
	}
	k := update(pglogrepl.UpdateMessageTupleTypeKey, &pglogrepl.TupleDataColumn{DataType: 'n'})
	require.Contains(t, k.UnchangedToastColumns, "big")
	require.NotContains(t, k.NewItems.ColToVal, "big")
	o := update(pglogrepl.UpdateMessageTupleTypeOld, &pglogrepl.TupleDataColumn{DataType: 't', Data: []byte("x")})
	require.NotContains(t, o.UnchangedToastColumns, "big")
	require.Equal(t, []byte("x"), o.NewItems.ColToVal["big"])
}
