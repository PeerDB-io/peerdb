package pua

import (
	"github.com/yuin/gopher-lua"
)

func FlatBuffers_Loader(ls *lua.LState) int {
	ls.PreloadModule("flatbuffers.binaryarray", FlatBuffers_BinaryArray_Loader)
	ls.PreloadModule("flatbuffers.builder", FlatBuffers_Builder_Loader)
	ls.PreloadModule("flatbuffers.numTypes", FlatBuffers_N_Loader)
	ls.PreloadModule("flatbuffers.view", FlatBuffers_View_Loader)

	m := ls.NewTable()

	ls.Push(ls.NewFunction(FlatBuffers_N_Loader))
	ls.Call(0, 1)
	m.RawSetString("N", ls.Get(-1))
	ls.Pop(1)

	ls.Push(ls.NewFunction(FlatBuffers_View_Loader))
	ls.Call(0, 1)
	m.RawSetString("view", ls.Get(-1))
	ls.Pop(1)

	ls.Push(ls.NewFunction(FlatBuffers_BinaryArray_Loader))
	ls.Call(0, 1)
	m.RawSetString("binaryArray", ls.Get(-1))
	ls.Pop(1)

	ls.Push(ls.NewFunction(FlatBuffers_Builder_Loader))
	ls.Call(0, 1)
	builder := ls.GetField(ls.Get(-1), "New")
	m.RawSetString("Builder", builder)
	ls.Pop(1)

	ls.Push(m)
	return 1
}
