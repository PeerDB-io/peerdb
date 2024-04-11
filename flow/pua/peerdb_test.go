package pua

import (
	"testing"

	"github.com/yuin/gopher-lua"

	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

func assert(t *testing.T, ls *lua.LState, source string) {
	t.Helper()
	err := ls.DoString(source)
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
}

func Test(t *testing.T) {
	t.Parallel()

	ls := lua.NewState(lua.Options{})
	RegisterTypes(ls)

	row := model.NewRecordItems(1)
	row.AddColumn("a", qvalue.QValueInt64{Val: 5040})
	ls.Env.RawSetString("row", LuaRow.New(ls, row))

	assert(t, ls, `
assert(require('bit32').band(173, 21) == 5)
assert(dofile == nil)
assert(loadfile == nil)

local uuid = peerdb.UUID("02030507-0b0d-1113-7f83-898b9597f1fb")
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

local dec102 = peerdb.Decimal("10.2")
local dec101 = peerdb.Decimal("10.1")
assert(tostring(dec102) == "10.2")
assert(dec101 < dec102)


local msgpack = require "msgpack"
assert(msgpack.encode(uuid) == string.char(0xc4, 16, 2, 3, 5, 7, 11, 13, 17, 19, 127, 131, 137, 139, 149, 151, 241, 251))

local json = require "json"
assert(json.encode(row) == "{\"a\":5040}")
`)
}
