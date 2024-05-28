package shared

import (
	"math/big"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/yuin/gopher-lua"

	"github.com/PeerDB-io/glua64"
)

var (
	LuaTime    = glua64.UserDataType[time.Time]{Name: "peerdb_time"}
	LuaUuid    = glua64.UserDataType[uuid.UUID]{Name: "peerdb_uuid"}
	LuaBigInt  = glua64.UserDataType[*big.Int]{Name: "peerdb_bigint"}
	LuaDecimal = glua64.UserDataType[decimal.Decimal]{Name: "peerdb_decimal"}
)

func SliceToLTable[T any](ls *lua.LState, s []T, f func(T) lua.LValue) *lua.LTable {
	tbl := ls.CreateTable(len(s), 0)
	tbl.Metatable = ls.GetTypeMetatable("Array")
	for idx, val := range s {
		tbl.RawSetInt(idx+1, f(val))
	}
	return tbl
}

func LTableToSlice[T any](ls *lua.LState, tbl *lua.LTable, f func(*lua.LState, lua.LValue) T) []T {
	tlen := tbl.Len()
	slice := make([]T, 0, tlen)
	for i := range tlen {
		slice = append(slice, f(ls, tbl.RawGetInt(i)))
	}
	return slice
}
