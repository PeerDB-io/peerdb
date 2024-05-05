package pua

import (
	"github.com/yuin/gopher-lua"

	"github.com/PeerDB-io/peer-flow/model"
)

func AttachToStream(ls *lua.LState, lfn *lua.LFunction, stream *model.QRecordStream) *model.QRecordStream {
	output := model.NewQRecordStream(0)
	go func() {
		schema := stream.Schema()
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
