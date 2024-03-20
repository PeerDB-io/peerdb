package pua_flatbuffers

import (
	"encoding/binary"
	"math"
	"strconv"

	"github.com/yuin/gopher-lua"

	"github.com/PeerDB-io/peer-flow/pua"
)

// Minimal API implemented for generated code

type Ntype = uint8

const (
	tyint   Ntype = 0
	tyfloat Ntype = 1
	tybool  Ntype = 2
)

type N struct {
	width  uint8
	signed bool
	ntype  Ntype
}

var (
	uint8n   = N{width: 1, signed: false, ntype: tyint}
	uint16n  = N{width: 2, signed: false, ntype: tyint}
	uint32n  = N{width: 4, signed: false, ntype: tyint}
	uint64n  = N{width: 8, signed: false, ntype: tyint}
	int8n    = N{width: 1, signed: true, ntype: tyint}
	int16n   = N{width: 2, signed: true, ntype: tyint}
	int32n   = N{width: 4, signed: true, ntype: tyint}
	int64n   = N{width: 8, signed: true, ntype: tyint}
	float32n = N{width: 4, signed: true, ntype: tyfloat}
	float64n = N{width: 8, signed: true, ntype: tyfloat}
	booln    = N{width: 1, signed: false, ntype: tybool}
)

func (n *N) PackU64(buf []byte, val uint64) {
	switch n.width {
	case 1:
		buf[0] = uint8(val)
	case 2:
		binary.LittleEndian.PutUint16(buf, uint16(val))
	case 4:
		binary.LittleEndian.PutUint32(buf, uint32(val))
	case 8:
		binary.LittleEndian.PutUint64(buf, val)
	default:
		panic("Invalid PackU64 width")
	}
}

func (n *N) Pack(ls *lua.LState, buf []byte, val lua.LValue) {
	switch n.ntype {
	case tyint:
		switch lv := val.(type) {
		case *lua.LUserData:
			switch v := lv.Value.(type) {
			case int64:
				n.PackU64(buf, uint64(v))
			case uint64:
				n.PackU64(buf, v)
			default:
				n.PackU64(buf, 0)
			}
		case lua.LNumber:
			n.PackU64(buf, uint64(lv))
		case lua.LString:
			sv := string(lv)
			u64, err := strconv.ParseUint(sv, 10, int(n.width)*8)
			if err != nil {
				i64, err := strconv.ParseInt(sv, 10, int(n.width)*8)
				if err != nil {
					n.PackU64(buf, 0)
				} else {
					n.PackU64(buf, uint64(i64))
				}
			} else {
				n.PackU64(buf, u64)
			}
		default:
			n.PackU64(buf, 0)
		}
	case tyfloat:
		switch lv := val.(type) {
		case *lua.LUserData:
			switch v := lv.Value.(type) {
			case int64:
				n.PackU64(buf, math.Float64bits(float64(v)))
			case uint64:
				n.PackU64(buf, math.Float64bits(float64(v)))
			default:
				n.PackU64(buf, 0)
			}
		case lua.LNumber:
			n.PackU64(buf, math.Float64bits(float64(lv)))
		case lua.LString:
			f64, err := strconv.ParseFloat(string(lv), int(n.width)*8)
			if err != nil {
				n.PackU64(buf, math.Float64bits(f64))
			} else {
				n.PackU64(buf, 0)
			}
		default:
			n.PackU64(buf, 0)
		}
	case tybool:
		if lua.LVIsFalse(val) {
			buf[0] = 0
		} else {
			buf[1] = 1
		}
	}
}

func (n *N) UnpackU64(buf []byte) uint64 {
	switch n.width {
	case 1:
		return uint64(buf[0])
	case 2:
		return uint64(binary.LittleEndian.Uint16(buf))
	case 4:
		return uint64(binary.LittleEndian.Uint32(buf))
	case 8:
		return binary.LittleEndian.Uint64(buf)
	}
	panic("invalid bitwidth")
}

func (n *N) Unpack(ls *lua.LState, buf []byte) lua.LValue {
	switch n.ntype {
	case tyint:
		if !n.signed && n.width < 8 {
			return lua.LNumber(n.UnpackU64(buf))
		}
		switch n.width {
		case 1:
			return lua.LNumber(int8(buf[0]))
		case 2:
			return lua.LNumber(int16(binary.LittleEndian.Uint16(buf)))
		case 4:
			return lua.LNumber(int32(binary.LittleEndian.Uint32(buf)))
		case 8:
			u64 := binary.LittleEndian.Uint64(buf)
			if n.signed {
				return pua.LuaI64.New(ls, int64(u64))
			} else {
				return pua.LuaU64.New(ls, u64)
			}
		}
	case tyfloat:
		if n.width == 4 {
			u32 := binary.LittleEndian.Uint32(buf)
			return lua.LNumber(math.Float32frombits(u32))
		} else {
			u64 := binary.LittleEndian.Uint64(buf)
			return lua.LNumber(math.Float64frombits(u64))
		}
	case tybool:
		return lua.LBool(buf[0] != 0)
	}
	panic("invalid numeric metatype")
}

var LuaN = pua.UserDataType[N]{Name: "flatbuffers_n"}

func N_Loader(ls *lua.LState) int {
	mtidx := ls.CreateTable(0, 1)
	mtidx.RawSetString("Unpack", ls.NewFunction(NUnpack))
	mt := LuaView.NewMetatable(ls)
	mt.RawSetString("__index", mtidx)

	uint16ud := LuaN.New(ls, uint16n)
	uint32ud := LuaN.New(ls, uint32n)
	int32ud := LuaN.New(ls, int32n)

	m := ls.NewTable()
	m.RawSetString("Uint8", LuaN.New(ls, uint8n))
	m.RawSetString("Uint16", uint16ud)
	m.RawSetString("Uint32", uint32ud)
	m.RawSetString("Uint64", LuaN.New(ls, uint64n))
	m.RawSetString("Int8", LuaN.New(ls, int8n))
	m.RawSetString("Int16", LuaN.New(ls, int16n))
	m.RawSetString("Int32", int32ud)
	m.RawSetString("Int64", LuaN.New(ls, int64n))
	m.RawSetString("Float32", LuaN.New(ls, float32n))
	m.RawSetString("Float64", LuaN.New(ls, float64n))
	m.RawSetString("Bool", LuaN.New(ls, booln))

	m.RawSetString("UOffsetT", uint32ud)
	m.RawSetString("VOffsetT", uint16ud)
	m.RawSetString("SOffsetT", int32ud)

	ls.Push(m)
	return 1
}

func NUnpack(ls *lua.LState) int {
	n := LuaN.StartMeta(ls)
	pos := max(CheckOffset(ls, 2), 1)
	var buf []byte
	switch v := ls.Get(1).(type) {
	case lua.LString:
		buf = []byte(v[pos-1:])
	case *lua.LUserData:
		if ba, ok := v.Value.([]byte); ok {
			buf = ba[pos-1:]
		} else {
			ls.RaiseError("Invalid buf userdata passed to unpack")
			return 0
		}
	default:
		ls.RaiseError("Invalid buf passed to unpack")
		return 0
	}
	ls.Push(n.Unpack(ls, buf))
	return 1
}
