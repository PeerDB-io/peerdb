package pua

import (
	"github.com/yuin/gopher-lua"
)

type LuaUserDataType[T any] struct{ Name string }

func (udt *LuaUserDataType[T]) New(ls *lua.LState, val T) *lua.LUserData {
	return &lua.LUserData{
		Value:     val,
		Env:       ls.Env,
		Metatable: udt.Metatable(ls),
	}
}

func (udt *LuaUserDataType[T]) NewMetatable(ls *lua.LState) *lua.LTable {
	return ls.NewTypeMetatable(udt.Name)
}

func (udt *LuaUserDataType[T]) Metatable(ls *lua.LState) lua.LValue {
	return ls.GetTypeMetatable(udt.Name)
}

func (udt *LuaUserDataType[T]) Check(ls *lua.LState, idx int) (*lua.LUserData, T) {
	ud := ls.CheckUserData(idx)
	val, ok := ud.Value.(T)
	if !ok {
		ls.RaiseError("Invalid " + udt.Name)
	}
	return ud, val
}

func (udt *LuaUserDataType[T]) StartMeta(ls *lua.LState) T {
	_, val := udt.Check(ls, 1)
	return val
}

func (udt *LuaUserDataType[T]) StartIndex(ls *lua.LState) (T, string) {
	return udt.StartMeta(ls), ls.CheckString(2)
}
