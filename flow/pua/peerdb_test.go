package pua

import (
	"testing"

	lua "github.com/yuin/gopher-lua"

	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
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

	row_empty_array := model.NewRecordItems(1)
	row_empty_array.AddColumn("a", qvalue.QValueArrayInt32{Val: nil})
	ls.Env.RawSetString("row_empty_array", LuaRow.New(ls, row_empty_array))

	assert(t, ls, `
assert(require('bit32').band(173, 21) == 5)
assert(dofile == nil)
assert(loadfile == nil)

assert(#row == 1)
assert(not peerdb.type(0))

local uuidstring = "02030507-0b0d-1113-7f83-898b9597f1fb"
local uuid = peerdb.UUID(uuidstring)
assert(peerdb.type(uuid) == "uuid.UUID")
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
assert(uuid == peerdb.UUID(uuidstring))
assert(tostring(uuid) == uuidstring)

local dec102 = peerdb.Decimal("10.2")
local dec101 = peerdb.Decimal("10.1")
assert(tostring(dec102) == "10.2")
assert(dec101 < dec102)
assert(dec101 <= dec102)
assert(dec101 ~= dec102)
assert(peerdb.Decimal(dec101) + dec101 == peerdb.Decimal("20.2"))
assert(dec102 + dec101 == peerdb.Decimal("20.3"))
assert(dec102 - dec101 == peerdb.Decimal("0.1"))
assert(dec102 * dec101 == peerdb.Decimal("103.02"))
assert(-dec101 == peerdb.Decimal("-10.1"))
assert(peerdb.Decimal("5") / peerdb.Decimal("2") == peerdb.Decimal("2.5"))
assert(peerdb.Decimal("5") % peerdb.Decimal("3") == peerdb.Decimal("2"))
assert(peerdb.Decimal("9") ^ peerdb.Decimal("2") == peerdb.Decimal("81"))
assert(dec101.float64 == 10.1)
assert(dec101.exponent == -1)
assert(tostring(dec101.coefficient) == "101")
assert(tostring(dec101.coefficient64) == "101")
assert(dec101.exponent == -1)
assert(tostring(dec101.int64) == "10")
assert(tostring(dec101.bigint) == "10")
assert(tostring(-dec101.bigint) == "-10")
assert(dec101.coefficient < dec102.coefficient)
assert(dec101.coefficient <= dec102.coefficient)
assert(dec101.coefficient ~= dec102.coefficient)
assert(dec101.coefficient.is64)
assert(dec101.coefficient.int64 == dec101.coefficient64)
assert(dec101.coefficient.sign == 1)
assert(not dec101.coefficient.nothing)
assert(not dec101.nothing)

assert(peerdb.tostring(peerdb.unix_epoch) == "1970-01-01 00:00:00 +0000 UTC")
assert(tostring(peerdb.unix_epoch) == "1970-01-01 00:00:00 +0000 UTC")

local unix123 = peerdb.Time(123456.789)
assert(peerdb.unix_epoch < unix123)
assert(peerdb.unix_epoch <= unix123)
assert(peerdb.unix_epoch ~= unix123)
assert(tostring(unix123.unix_nano) == "123456789000000")
assert(tostring(unix123.unix_micro) == "123456789000")
assert(tostring(unix123.unix_milli) == "123456789")
assert(tostring(unix123.unix_second) == "123456")
assert(unix123.unix == 123456.789)
assert(unix123.year == 1970)
assert(unix123.month == 1)
assert(unix123.day == 2)
assert(unix123.yearday == 2)
assert(unix123.hour == 10)
assert(unix123.minute == 17)
assert(unix123.second == 36)
assert(unix123.nanosecond == 789000000)
assert(not unix123.nothing)

local msgpack = require "msgpack"
assert(msgpack.encode(uuid) == string.char(0xc4, 16, 2, 3, 5, 7, 11, 13, 17, 19, 127, 131, 137, 139, 149, 151, 241, 251))
assert(msgpack.encode(row_empty_array.a) == string.char(0x90))

local json = require "json"
assert(json.encode(row) == "{\"a\":5040}")
assert(json.encode(row_empty_array.a) == "[]")
`)
}
