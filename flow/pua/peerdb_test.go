package pua

import (
	"testing"

	"github.com/google/uuid"
	"github.com/yuin/gopher-lua"
)

func assert(t *testing.T, ls *lua.LState, source string) {
	t.Helper()
	err := ls.DoString(source)
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
}

func Test_Lua(t *testing.T) {
	t.Parallel()

	ls := lua.NewState(lua.Options{})
	RegisterTypes(ls)

	id := uuid.UUID([16]byte{2, 3, 5, 7, 11, 13, 17, 19, 127, 131, 137, 139, 149, 151, 241, 251})
	ls.Env.RawSetString("uuid", LuaUuid.New(ls, id))

	n5 := int64(-5)
	ls.Env.RawSetString("i64p5", LuaI64.New(ls, 5))
	ls.Env.RawSetString("i64p5_2", LuaI64.New(ls, 5))
	ls.Env.RawSetString("u64p5", LuaU64.New(ls, 5))
	ls.Env.RawSetString("u64p5_2", LuaU64.New(ls, 5))
	ls.Env.RawSetString("i64n5", LuaI64.New(ls, n5))
	ls.Env.RawSetString("u64n5", LuaU64.New(ls, uint64(n5)))

	assert(t, ls, `
assert(require('bit32').band(173, 21) == 5)
assert(dofile == nil)
assert(loadfile == nil)

assert(uuid[0] == 2)
assert(uuid[1] == 3)
assert(uuid[2] == 5)
assert(uuid[3] == 7)
assert(uuid[4] == 11)
assert(uuid[5] == 13)
assert(uuid[6] == 17)
assert(uuid[7] == 19)
assert(uuid[8] == 127)
assert(uuid[9] == 131)
assert(uuid[10] == 137)
assert(uuid[11] == 139)
assert(uuid[12] == 149)
assert(uuid[13] == 151)
assert(uuid[14] == 241)
assert(uuid[15] == 251)

print(i64p5, u64p5)
assert(i64p5 == u64p5)
assert(i64p5 ~= i64n5)
assert(i64n5 ~= u64n5)
assert(i64p5 == i64p5_2)
assert(u64p5 == u64p5_2)
assert(u64n5 > i64p5)
assert(i64p5 > i64n5)
`)
}
