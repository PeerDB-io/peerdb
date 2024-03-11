package pua

import (
	"encoding/binary"
	"math"
	"strconv"

	"github.com/yuin/gopher-lua"
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
				return LuaI64.New(ls, int64(u64))
			} else {
				return LuaU64.New(ls, u64)
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

var LuaN = LuaUserDataType[N]{Name: "flatbuffers_n"}

func FlatBuffers_N_Loader(ls *lua.LState) int {
	m := ls.NewTable()

	mt := LuaView.NewMetatable(ls)
	ls.SetField(mt, "__index", ls.NewFunction(NIndex))

	uint16ud := LuaN.New(ls, uint16n)
	uint32ud := LuaN.New(ls, uint32n)
	int32ud := LuaN.New(ls, int32n)

	ls.SetField(m, "Uint8", LuaN.New(ls, uint8n))
	ls.SetField(m, "Uint16", uint16ud)
	ls.SetField(m, "Uint32", uint32ud)
	ls.SetField(m, "Uint64", LuaN.New(ls, uint64n))
	ls.SetField(m, "Int8", LuaN.New(ls, int8n))
	ls.SetField(m, "Int16", LuaN.New(ls, int16n))
	ls.SetField(m, "Int32", int32ud)
	ls.SetField(m, "Int64", LuaN.New(ls, int64n))
	ls.SetField(m, "Float32", LuaN.New(ls, float32n))
	ls.SetField(m, "Float64", LuaN.New(ls, float64n))
	ls.SetField(m, "Bool", LuaN.New(ls, booln))

	ls.SetField(m, "UOffsetT", uint32ud)
	ls.SetField(m, "VOffsetT", uint16ud)
	ls.SetField(m, "SOffsetT", int32ud)

	ls.Push(m)
	return 1
}

func NIndex(ls *lua.LState) int {
	n, key := LuaN.StartIndex(ls)
	if key == "Unpack" {
		var buf []byte
		switch v := ls.Get(1).(type) {
		case lua.LString:
			buf = []byte(v)
		case *lua.LUserData:
			ba, ok := v.Value.(BinaryArray)
			if ok {
				buf = ba.data
			} else {
				ls.RaiseError("Invalid buf userdata passed to unpack")
				return 0
			}
		default:
			ls.RaiseError("Invalid buf passed to unpack")
			return 0
		}
		pos := max(CheckOffset(ls, 2), 1)
		ls.Push(n.Unpack(ls, buf[pos-1:]))
		return 1
	} else {
		ls.RaiseError("Unsupported field on N: " + key)
		return 0
	}
}
