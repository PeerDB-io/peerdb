package pua

import (
	"github.com/yuin/gopher-lua"
)

type BinaryArray struct {
	data []byte
}

var LuaBinaryArray = LuaUserDataType[BinaryArray]{Name: "flatbuffers_binaryarray"}

func FlatBuffers_BinaryArray_Loader(ls *lua.LState) int {
	m := ls.NewTable()
	ls.SetField(m, "New", ls.NewFunction(BinaryArrayNew))

	mt := LuaBinaryArray.NewMetatable(ls)
	ls.SetField(mt, "__index", ls.NewFunction(BinaryArrayIndex))
	ls.SetField(mt, "__len", ls.NewFunction(BinaryArrayLen))
	ls.SetField(mt, "Slice", ls.NewFunction(BinaryArraySlice))
	ls.SetField(mt, "Grow", ls.NewFunction(BinaryArrayGrow))
	ls.SetField(mt, "Pad", ls.NewFunction(BinaryArrayPad))
	ls.SetField(mt, "Set", ls.NewFunction(BinaryArraySet))

	ls.Push(m)
	return 1
}

func BinaryArrayNew(ls *lua.LState) int {
	lval := ls.Get(-1)
	var ba BinaryArray
	switch val := lval.(type) {
	case lua.LString:
		ba = BinaryArray{
			data: []byte(val),
		}
	case lua.LNumber:
		ba = BinaryArray{
			data: make([]byte, int(val)),
		}
	default:
		ls.RaiseError("Expect a integer size value or string to construct a binary array")
		return 0
	}
	ls.Push(LuaBinaryArray.New(ls, ba))
	return 1
}

func BinaryArrayLen(ls *lua.LState) int {
	ba := LuaBinaryArray.StartMeta(ls)
	ls.Push(lua.LNumber(len(ba.data)))
	return 1
}

func BinaryArrayIndex(ls *lua.LState) int {
	ba, key := LuaBinaryArray.StartIndex(ls)
	switch key {
	case "size":
		ls.Push(lua.LNumber(len(ba.data)))
	case "str":
		ls.Push(lua.LString(ba.data))
	case "data":
		ls.RaiseError("BinaryArray data property inaccessible")
		return 0
	default:
		ls.Push(ls.GetField(LuaBinaryArray.Metatable(ls), key))
	}
	return 1
}

func BinaryArraySlice(ls *lua.LState) int {
	var startPos, endPos int
	ba := LuaBinaryArray.StartMeta(ls)
	if luaStartPos, ok := ls.Get(2).(lua.LNumber); ok {
		startPos = max(int(luaStartPos), 0)
	} else {
		startPos = 0
	}
	if luaEndPos, ok := ls.Get(3).(lua.LNumber); ok {
		endPos = min(int(luaEndPos), len(ba.data))
	} else {
		endPos = len(ba.data)
	}
	ls.Push(lua.LString(ba.data[startPos:endPos]))
	return 1
}

func (ba *BinaryArray) Grow(newsize int) {
	newdata := make([]byte, newsize)
	copy(newdata[newsize-len(ba.data):], ba.data)
	ba.data = newdata
}

func BinaryArrayGrow(ls *lua.LState) int {
	baud, ba := LuaBinaryArray.Check(ls, 1)
	newsize := int(ls.CheckNumber(2))
	if newsize > len(ba.data) {
		ba.Grow(newsize)
		baud.Value = ba
	}
	return 0
}

func (ba *BinaryArray) Pad(n int, start int) {
	for i := range n {
		ba.data[start+i] = 0
	}
}

func BinaryArrayPad(ls *lua.LState) int {
	ba := LuaBinaryArray.StartMeta(ls)
	n := int(ls.CheckNumber(2))
	startPos := int(ls.CheckNumber(3))
	ba.Pad(n, startPos)
	return 0
}

func BinaryArraySet(ls *lua.LState) int {
	ba := LuaBinaryArray.StartMeta(ls)
	idx := int(ls.CheckNumber(3))
	value := ls.Get(2)
	if num, ok := value.(lua.LNumber); ok {
		ba.data[idx] = byte(num)
	}
	if str, ok := value.(lua.LString); ok {
		ba.data[idx] = str[0]
	}
	return 0
}
