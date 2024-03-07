package pua

import (
	"github.com/yuin/gopher-lua"
)

/*
local m = {}

m.Builder = require("flatbuffers.builder").New
m.N = require("flatbuffers.numTypes")
m.view = require("flatbuffers.view")
m.binaryArray = require("flatbuffers.binaryarray")

return m
*/

func requireHelper(ls *lua.LState, m *lua.LTable, require lua.LValue, name string, path string) {
	ls.Push(require)
	ls.Push(lua.LString(path))
	ls.Call(1, 1)
	ls.SetField(m, name, ls.Get(-1))
	ls.Pop(1)
}

func FlatBuffers_Loader(ls *lua.LState) int {
	ls.PreloadModule("flatbuffers.binaryarray", FlatBuffers_BinaryArray_Loader)
	ls.PreloadModule("flatbuffers.builder", FlatBuffers_Builder_Loader)
	ls.PreloadModule("flatbuffers.numTypes", FlatBuffers_N_Loader)
	ls.PreloadModule("flatbuffers.view", FlatBuffers_View_Loader)

	m := ls.NewTable()
	require := ls.GetGlobal("require")
	ls.Push(require)
	ls.Push(lua.LString("flatbuffers.builder"))
	ls.Call(1, 1)
	builder := ls.GetTable(ls.Get(-1), lua.LString("New"))
	ls.SetField(m, "builder", builder)
	ls.Pop(1)

	requireHelper(ls, m, require, "N", "flatbuffers.numTypes")
	requireHelper(ls, m, require, "view", "flatbuffers.view")
	requireHelper(ls, m, require, "binaryArray", "flatbuffers.binaryarray")

	ls.Push(m)
	return 1
}
