package pua_flatbuffers

import (
	"github.com/yuin/gopher-lua"
)

func Loader(ls *lua.LState) int {
	ls.PreloadModule("flatbuffers.binaryarray", BinaryArray_Loader)
	ls.PreloadModule("flatbuffers.builder", Builder_Loader)
	ls.PreloadModule("flatbuffers.numTypes", N_Loader)
	ls.PreloadModule("flatbuffers.view", View_Loader)

	m := ls.NewTable()

	ls.Push(ls.NewFunction(N_Loader))
	ls.Call(0, 1)
	m.RawSetString("N", ls.Get(-1))
	ls.Pop(1)

	ls.Push(ls.NewFunction(View_Loader))
	ls.Call(0, 1)
	m.RawSetString("view", ls.Get(-1))
	ls.Pop(1)

	ls.Push(ls.NewFunction(BinaryArray_Loader))
	ls.Call(0, 1)
	m.RawSetString("binaryArray", ls.Get(-1))
	ls.Pop(1)

	ls.Push(ls.NewFunction(Builder_Loader))
	ls.Call(0, 1)
	builder := ls.GetField(ls.Get(-1), "New")
	m.RawSetString("Builder", builder)
	ls.Pop(1)

	ls.Push(m)
	return 1
}
