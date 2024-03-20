package pua

import (
	"github.com/yuin/gopher-lua"
)

type UserDataType[T any] struct{ Name string }

func (udt *UserDataType[T]) New(ls *lua.LState, val T) *lua.LUserData {
	return &lua.LUserData{
		Value:     val,
		Env:       ls.Env,
		Metatable: udt.Metatable(ls),
	}
}

func (udt *UserDataType[T]) NewMetatable(ls *lua.LState) *lua.LTable {
	return ls.NewTypeMetatable(udt.Name)
}

func (udt *UserDataType[T]) Metatable(ls *lua.LState) lua.LValue {
	return ls.GetTypeMetatable(udt.Name)
}

func (udt *UserDataType[T]) Check(ls *lua.LState, idx int) (*lua.LUserData, T) {
	ud := ls.CheckUserData(idx)
	val, ok := ud.Value.(T)
	if !ok {
		ls.RaiseError("Invalid " + udt.Name)
	}
	return ud, val
}

func (udt *UserDataType[T]) StartMeta(ls *lua.LState) T {
	_, val := udt.Check(ls, 1)
	return val
}

func (udt *UserDataType[T]) StartIndex(ls *lua.LState) (T, string) {
	return udt.StartMeta(ls), ls.CheckString(2)
}
