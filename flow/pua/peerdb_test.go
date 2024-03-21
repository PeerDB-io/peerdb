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
`)
}
