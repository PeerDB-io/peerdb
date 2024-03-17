package pua

import (
	"bytes"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
	"github.com/yuin/gopher-lua"

	"github.com/PeerDB-io/peer-flow/connectors/utils/catalog"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

var (
	LuaRecord  = LuaUserDataType[model.Record]{Name: "peerdb_record"}
	LuaRow     = LuaUserDataType[*model.RecordItems]{Name: "peerdb_row"}
	LuaQValue  = LuaUserDataType[qvalue.QValue]{Name: "peerdb_value"}
	LuaI64     = LuaUserDataType[int64]{Name: "flatbuffers_i64"}
	LuaU64     = LuaUserDataType[uint64]{Name: "flatbuffers_u64"}
	LuaTime    = LuaUserDataType[time.Time]{Name: "peerdb_time"}
	LuaUuid    = LuaUserDataType[uuid.UUID]{Name: "peerdb_uuid"}
	LuaBigInt  = LuaUserDataType[*big.Int]{Name: "peerdb_bigint"}
	LuaDecimal = LuaUserDataType[decimal.Decimal]{Name: "peerdb_bigrat"}
)

func RegisterTypes(ls *lua.LState) {
	// gopher-lua provides 2 loaders {preload, file}
	// overwrite file loader with one retrieving scripts from database
	loaders := ls.G.Registry.RawGetString("_LOADERS").(*lua.LTable)
	loaders.RawSetInt(2, ls.NewFunction(LoadPeerdbScript))

	mt := LuaRecord.NewMetatable(ls)
	mt.RawSetString("__index", ls.NewFunction(LuaRecordIndex))

	mt = LuaRow.NewMetatable(ls)
	mt.RawSetString("__index", ls.NewFunction(LuaRowIndex))
	mt.RawSetString("__len", ls.NewFunction(LuaRowLen))

	mt = LuaQValue.NewMetatable(ls)
	mt.RawSetString("__index", ls.NewFunction(LuaQValueIndex))
	mt.RawSetString("__tostring", ls.NewFunction(LuaQValueString))
	mt.RawSetString("__len", ls.NewFunction(LuaQValueLen))

	mt = LuaUuid.NewMetatable(ls)
	mt.RawSetString("__index", ls.NewFunction(LuaUuidIndex))
	mt.RawSetString("__tostring", ls.NewFunction(LuaUuidString))

	mt = LuaI64.NewMetatable(ls)
	mt.RawSetString("__index", ls.NewFunction(LuaI64Index))
	mt.RawSetString("__tostring", ls.NewFunction(LuaI64String))
	mt.RawSetString("__eq", ls.NewFunction(Lua64Eq))
	mt.RawSetString("__le", ls.NewFunction(Lua64Le))
	mt.RawSetString("__lt", ls.NewFunction(Lua64Lt))

	mt = LuaU64.NewMetatable(ls)
	mt.RawSetString("__index", ls.NewFunction(LuaU64Index))
	mt.RawSetString("__tostring", ls.NewFunction(LuaU64String))
	mt.RawSetString("__eq", ls.NewFunction(Lua64Eq))
	mt.RawSetString("__le", ls.NewFunction(Lua64Le))
	mt.RawSetString("__lt", ls.NewFunction(Lua64Lt))

	mt = LuaTime.NewMetatable(ls)
	mt.RawSetString("__index", ls.NewFunction(LuaTimeIndex))
	mt.RawSetString("__tostring", ls.NewFunction(LuaTimeString))

	mt = LuaBigInt.NewMetatable(ls)
	mt.RawSetString("__index", ls.NewFunction(LuaBigIntIndex))
	mt.RawSetString("__tostring", ls.NewFunction(LuaBigIntString))
	mt.RawSetString("__len", ls.NewFunction(LuaBigIntLen))

	mt = LuaDecimal.NewMetatable(ls)
	mt.RawSetString("__index", ls.NewFunction(LuaDecimalIndex))
	mt.RawSetString("__tostring", ls.NewFunction(LuaDecimalString))

	peerdb := ls.NewTable()
	peerdb.RawSetString("RowToJSON", ls.NewFunction(LuaRowToJSON))
	peerdb.RawSetString("RowColumns", ls.NewFunction(LuaRowColumns))
	peerdb.RawSetString("Now", ls.NewFunction(LuaNow))
	peerdb.RawSetString("UUID", ls.NewFunction(LuaUUID))
	ls.Env.RawSetString("peerdb", peerdb)
}

func LoadPeerdbScript(ls *lua.LState) int {
	ctx := ls.Context()
	name := ls.CheckString(1)
	pool, err := utils.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		ls.RaiseError("Connection failed loading %s: %s", name, err.Error())
		return 0
	}

	var source []byte
	err = pool.QueryRow(ctx, "select source from scripts where lang = 'lua' and name = $1", name).Scan(&source)
	if err != nil {
		if err == pgx.ErrNoRows {
			ls.Push(lua.LString("Could not find script " + name))
			return 1
		}
		ls.RaiseError("Failed to load script %s: %s", name, err.Error())
		return 0
	}

	fn, err := ls.Load(bytes.NewReader(source), name)
	if err != nil {
		ls.RaiseError(err.Error())
	}
	ls.Push(fn)
	return 1
}

func LuaRowIndex(ls *lua.LState) int {
	row, key := LuaRow.StartIndex(ls)

	qv, err := row.GetValueByColName(key)
	if err != nil {
		ls.RaiseError(err.Error())
		return 0
	}

	ls.Push(LuaQValue.New(ls, qv))
	return 1
}

func LuaRowLen(ls *lua.LState) int {
	_, row := LuaRow.Check(ls, 1)
	ls.Push(lua.LNumber(len(row.Values)))
	return 1
}

func LuaRowToJSON(ls *lua.LState) int {
	_, row := LuaRow.Check(ls, 1)
	json, err := row.ToJSON()
	if err != nil {
		ls.RaiseError("failed to serialize json: %s", err.Error())
		return 0
	}
	ls.Push(lua.LString(json))
	return 1
}

func LuaRowColumns(ls *lua.LState) int {
	_, row := LuaRow.Check(ls, 1)
	tbl := ls.CreateTable(len(row.ColToValIdx), 0)
	for col, idx := range row.ColToValIdx {
		tbl.RawSetInt(idx, lua.LString(col))
	}
	ls.Push(tbl)
	return 1
}

func LuaRecordIndex(ls *lua.LState) int {
	record, key := LuaRecord.StartIndex(ls)
	switch key {
	case "kind":
		switch record.(type) {
		case *model.InsertRecord:
			ls.Push(lua.LString("insert"))
		case *model.UpdateRecord:
			ls.Push(lua.LString("update"))
		case *model.DeleteRecord:
			ls.Push(lua.LString("delete"))
		case *model.RelationRecord:
			ls.Push(lua.LString("relation"))
		}
	case "row":
		items := record.GetItems()
		if items != nil {
			ls.Push(LuaRow.New(ls, items))
		} else {
			ls.Push(lua.LNil)
		}
	case "old":
		var items *model.RecordItems
		switch rec := record.(type) {
		case *model.UpdateRecord:
			items = rec.OldItems
		case *model.DeleteRecord:
			items = rec.Items
		}
		if items != nil {
			ls.Push(LuaRow.New(ls, items))
		} else {
			ls.Push(lua.LNil)
		}
	case "new":
		var items *model.RecordItems
		switch rec := record.(type) {
		case *model.InsertRecord:
			items = rec.Items
		case *model.UpdateRecord:
			items = rec.NewItems
		}
		if items != nil {
			ls.Push(LuaRow.New(ls, items))
		} else {
			ls.Push(lua.LNil)
		}
	case "checkpoint":
		ls.Push(LuaI64.New(ls, record.GetCheckpointID()))
	case "target":
		ls.Push(lua.LString(record.GetDestinationTableName()))
	case "source":
		ls.Push(lua.LString(record.GetSourceTableName()))
	default:
		return 0
	}
	return 1
}

func qvToLTable[T any](ls *lua.LState, s []T, f func(x T) lua.LValue) {
	tbl := ls.CreateTable(len(s), 0)
	for idx, val := range s {
		tbl.RawSetInt(idx, f(val))
	}
	ls.Push(tbl)
}

func LuaQValueIndex(ls *lua.LState) int {
	qv, key := LuaQValue.StartIndex(ls)
	switch key {
	case "kind":
		ls.Push(lua.LString(qv.Kind))
	case "value":
		switch v := qv.Value.(type) {
		case nil:
			ls.Push(lua.LNil)
		case bool:
			ls.Push(lua.LBool(v))
		case uint8:
			if qv.Kind == qvalue.QValueKindQChar {
				ls.Push(lua.LString(rune(v)))
			} else {
				ls.Push(lua.LNumber(v))
			}
		case int16:
			ls.Push(lua.LNumber(v))
		case int32:
			ls.Push(lua.LNumber(v))
		case int64:
			ls.Push(LuaI64.New(ls, v))
		case float32:
			ls.Push(lua.LNumber(v))
		case float64:
			ls.Push(lua.LNumber(v))
		case string:
			if qv.Kind == qvalue.QValueKindUUID {
				u, err := uuid.Parse(v)
				if err != nil {
					ls.Push(LuaUuid.New(ls, u))
				} else {
					ls.Push(lua.LString(v))
				}
			} else {
				ls.Push(lua.LString(v))
			}
		case time.Time:
			ls.Push(LuaTime.New(ls, v))
		case decimal.Decimal:
			ls.Push(LuaDecimal.New(ls, v))
		case [16]byte:
			ls.Push(LuaUuid.New(ls, uuid.UUID(v)))
		case []byte:
			ls.Push(lua.LString(v))
		case []float32:
			qvToLTable(ls, v, func(f float32) lua.LValue {
				return lua.LNumber(f)
			})
		case []float64:
			qvToLTable(ls, v, func(f float64) lua.LValue {
				return lua.LNumber(f)
			})
		case []int16:
			qvToLTable(ls, v, func(f int16) lua.LValue {
				return lua.LNumber(f)
			})
		case []int32:
			qvToLTable(ls, v, func(f int32) lua.LValue {
				return lua.LNumber(f)
			})
		case []int64:
			qvToLTable(ls, v, func(x int64) lua.LValue {
				return LuaI64.New(ls, x)
			})
		case []string:
			qvToLTable(ls, v, func(x string) lua.LValue {
				return lua.LString(x)
			})
		case []time.Time:
			qvToLTable(ls, v, func(x time.Time) lua.LValue {
				return LuaTime.New(ls, x)
			})
		case []bool:
			qvToLTable(ls, v, func(x bool) lua.LValue {
				return lua.LBool(x)
			})
		}
	case "int64":
		ls.Push(LuaI64.New(ls, reflect.ValueOf(qv.Value).Int()))
	case "float64":
		ls.Push(lua.LNumber(reflect.ValueOf(qv.Value).Float()))
	default:
		return 0
	}
	return 1
}

func LuaQValueLen(ls *lua.LState) int {
	qv := LuaQValue.StartMeta(ls)
	str, ok := qv.Value.(string)
	if ok {
		ls.Push(lua.LNumber(len(str)))
		return 1
	}
	if strings.HasPrefix(string(qv.Kind), "array_") {
		ls.Push(lua.LNumber(reflect.ValueOf(qv.Value).Len()))
		return 1
	}
	return 0
}

func LuaQValueString(ls *lua.LState) int {
	qv := LuaQValue.StartMeta(ls)
	ls.Push(lua.LString(fmt.Sprint(qv.Value)))
	return 1
}

func LuaUuidIndex(ls *lua.LState) int {
	_, val := LuaUuid.Check(ls, 1)
	key := ls.CheckNumber(2)
	ki := int(key)
	if ki >= 0 && ki < 16 {
		ls.Push(lua.LNumber(val[ki]))
		return 1
	}
	return 0
}

func LuaUuidString(ls *lua.LState) int {
	val := LuaUuid.StartMeta(ls)
	ls.Push(lua.LString(val.String()))
	return 1
}

func LuaNow(ls *lua.LState) int {
	ls.Push(LuaTime.New(ls, time.Now()))
	return 1
}

func LuaUUID(ls *lua.LState) int {
	ls.Push(LuaUuid.New(ls, uuid.New()))
	return 1
}

func Lua64Eq(ls *lua.LState) int {
	aud := ls.CheckUserData(1)
	bud := ls.CheckUserData(2)
	switch a := aud.Value.(type) {
	case int64:
		switch b := bud.Value.(type) {
		case int64:
			ls.Push(lua.LBool(a == b))
		case uint64:
			if a < 0 {
				ls.Push(lua.LFalse)
			} else {
				ls.Push(lua.LBool(uint64(a) == b))
			}
		default:
			return 0
		}
	case uint64:
		switch b := bud.Value.(type) {
		case int64:
			if b < 0 {
				ls.Push(lua.LFalse)
			} else {
				ls.Push(lua.LBool(a == uint64(b)))
			}
		case uint64:
			ls.Push(lua.LBool(a == b))
		default:
			return 0
		}
	default:
		return 0
	}
	return 1
}

func Lua64Le(ls *lua.LState) int {
	aud := ls.CheckUserData(1)
	bud := ls.CheckUserData(2)
	switch a := aud.Value.(type) {
	case int64:
		switch b := bud.Value.(type) {
		case int64:
			ls.Push(lua.LBool(a <= b))
		case uint64:
			if a < 0 {
				ls.Push(lua.LTrue)
			} else {
				ls.Push(lua.LBool(uint64(a) <= b))
			}
		default:
			return 0
		}
	case uint64:
		switch b := bud.Value.(type) {
		case int64:
			if b < 0 {
				ls.Push(lua.LFalse)
			} else {
				ls.Push(lua.LBool(a <= uint64(b)))
			}
		case uint64:
			ls.Push(lua.LBool(a <= b))
		default:
			return 0
		}
	default:
		return 0
	}
	return 1
}

func Lua64Lt(ls *lua.LState) int {
	aud := ls.CheckUserData(1)
	bud := ls.CheckUserData(2)
	switch a := aud.Value.(type) {
	case int64:
		switch b := bud.Value.(type) {
		case int64:
			ls.Push(lua.LBool(a < b))
		case uint64:
			if a < 0 {
				ls.Push(lua.LTrue)
			} else {
				ls.Push(lua.LBool(uint64(a) < b))
			}
		default:
			return 0
		}
	case uint64:
		switch b := bud.Value.(type) {
		case int64:
			if b < 0 {
				ls.Push(lua.LTrue)
			} else {
				ls.Push(lua.LBool(a < uint64(b)))
			}
		case uint64:
			ls.Push(lua.LBool(a < b))
		default:
			return 0
		}
	default:
		return 0
	}
	return 1
}

func LuaI64Index(ls *lua.LState) int {
	i64ud, i64 := LuaI64.Check(ls, 1)
	key := ls.CheckString(2)
	switch key {
	case "i64":
		ls.Push(i64ud)
	case "u64":
		ls.Push(LuaU64.New(ls, uint64(i64)))
	case "float64":
		ls.Push(lua.LNumber(i64))
	default:
		return 0
	}
	return 1
}

func LuaU64Index(ls *lua.LState) int {
	u64ud, u64 := LuaU64.Check(ls, 1)
	key := ls.CheckString(2)
	switch key {
	case "i64":
		ls.Push(LuaI64.New(ls, int64(u64)))
	case "u64":
		ls.Push(u64ud)
	case "float64":
		ls.Push(lua.LNumber(u64))
	default:
		return 0
	}
	return 1
}

func LuaI64String(ls *lua.LState) int {
	i64 := LuaI64.StartMeta(ls)
	ls.Push(lua.LString(strconv.FormatInt(i64, 10)))
	return 1
}

func LuaU64String(ls *lua.LState) int {
	u64 := LuaU64.StartMeta(ls)
	ls.Push(lua.LString(strconv.FormatUint(u64, 10)))
	return 1
}

func LuaTimeIndex(ls *lua.LState) int {
	tm, key := LuaTime.StartIndex(ls)
	switch key {
	case "unix_nano":
		ls.Push(LuaI64.New(ls, tm.UnixNano()))
	case "unix_micro":
		ls.Push(LuaI64.New(ls, tm.UnixMicro()))
	case "unix_milli":
		ls.Push(LuaI64.New(ls, tm.UnixMilli()))
	case "unix":
		ls.Push(LuaI64.New(ls, tm.Unix()))
	case "year":
		ls.Push(lua.LNumber(tm.Year()))
	case "month":
		ls.Push(lua.LNumber(tm.Month()))
	case "day":
		ls.Push(lua.LNumber(tm.Day()))
	case "yearday":
		ls.Push(lua.LNumber(tm.YearDay()))
	case "hour":
		ls.Push(lua.LNumber(tm.Hour()))
	case "minute":
		ls.Push(lua.LNumber(tm.Minute()))
	case "second":
		ls.Push(lua.LNumber(tm.Second()))
	case "nanosecond":
		ls.Push(lua.LNumber(tm.Nanosecond()))
	default:
		return 0
	}
	return 1
}

func LuaTimeString(ls *lua.LState) int {
	tm := LuaTime.StartMeta(ls)
	ls.Push(lua.LString(tm.String()))
	return 1
}

func LuaBigIntIndex(ls *lua.LState) int {
	_, bi := LuaBigInt.Check(ls, 1)
	switch key := ls.Get(2).(type) {
	case lua.LNumber:
		ls.Push(lua.LNumber(bi.Bytes()[int(key)]))
	case lua.LString:
		switch string(key) {
		case "sign":
			ls.Push(lua.LNumber(bi.Sign()))
		case "bytes":
			ls.Push(lua.LString(bi.Bytes()))
		case "int64":
			ls.Push(LuaI64.New(ls, bi.Int64()))
		case "is64":
			ls.Push(lua.LBool(bi.IsInt64()))
		}
	default:
		ls.RaiseError("BigInt accessed with non number/string")
	}
	return 1
}

func LuaBigIntString(ls *lua.LState) int {
	bi := LuaBigInt.StartMeta(ls)
	ls.Push(lua.LString(bi.String()))
	return 1
}

func LuaBigIntLen(ls *lua.LState) int {
	bi := LuaBigInt.StartMeta(ls)
	ls.Push(lua.LNumber(len(bi.Bytes())))
	return 1
}

func LuaDecimalIndex(ls *lua.LState) int {
	num, key := LuaDecimal.StartIndex(ls)
	switch key {
	case "coefficient":
		ls.Push(LuaBigInt.New(ls, num.Coefficient()))
	case "coefficient64":
		ls.Push(LuaI64.New(ls, num.CoefficientInt64()))
	case "exponent":
		ls.Push(lua.LNumber(num.Exponent()))
	case "bigint":
		ls.Push(LuaBigInt.New(ls, num.BigInt()))
	case "int64":
		ls.Push(LuaI64.New(ls, num.IntPart()))
	case "float64":
		f64, _ := num.Float64()
		ls.Push(lua.LNumber(f64))
	default:
		return 0
	}
	return 1
}

func LuaDecimalString(ls *lua.LState) int {
	num := LuaDecimal.StartMeta(ls)
	ls.Push(lua.LString(num.String()))
	return 1
}
