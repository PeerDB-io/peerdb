package pua

import (
	"context"

	lua "github.com/yuin/gopher-lua"

	"github.com/PeerDB-io/peerdb/flow/model"
)

func AttachToStream(ls *lua.LState, lfn *lua.LFunction, stream *model.QRecordStream) *model.QRecordStream {
	output := model.NewQRecordStream(0)
	go func() {
		schema, err := stream.Schema()
		if err != nil {
			output.Close(err)
			return
		}
		output.SetSchema(schema)
		for record := range stream.Records {
			row := model.NewRecordItems(len(record))
			for i, qv := range record {
				row.AddColumn(schema.Fields[i].Name, qv)
			}
			ls.Push(lfn)
			ls.Push(LuaRow.New(ls, row))
			if err := ls.PCall(1, 0, nil); err != nil {
				output.Close(err)
				return
			}
			for i, field := range schema.Fields {
				record[i] = row.GetColumnValue(field.Name)
			}
			output.Records <- record
		}
		output.Close(stream.Err())
	}()
	return output
}

func AttachToCdcStream(
	ctx context.Context,
	ls *lua.LState,
	lfn *lua.LFunction,
	stream *model.CDCStream[model.RecordItems],
	onErr context.CancelCauseFunc,
) *model.CDCStream[model.RecordItems] {
	outstream := model.NewCDCStream[model.RecordItems](0)

	handleErr := func(err error) {
		onErr(err)
		<-ctx.Done()
		for range stream.GetRecords() {
			// still read records to make sure input closes first
		}
	}

	go func() {
		if stream.WaitAndCheckEmpty() {
			outstream.SignalAsEmpty()
			<-stream.GetRecords() // needed because empty signal comes before Close
		} else {
			outstream.SignalAsNotEmpty()
			for record := range stream.GetRecords() {
				ls.Push(lfn)
				ls.Push(LuaRecord.New(ls, record))
				if err := ls.PCall(1, 0, nil); err != nil {
					handleErr(err)
					break
				}
				err := outstream.AddRecord(ctx, record)
				if err != nil {
					handleErr(err)
					break
				}
			}
		}
		outstream.SchemaDeltas = stream.SchemaDeltas
		lastCP := stream.GetLastCheckpoint()
		outstream.UpdateLatestCheckpointID(lastCP.ID)
		outstream.UpdateLatestCheckpointText(lastCP.Text)
		outstream.Close()
	}()
	return outstream
}
