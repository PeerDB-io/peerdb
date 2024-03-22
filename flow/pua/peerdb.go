package pua

import (
	"bytes"
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
	"github.com/yuin/gopher-lua"

	"github.com/PeerDB-io/glua64"
	"github.com/PeerDB-io/gluabit32"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

var (
	LuaRecord  = glua64.UserDataType[model.Record]{Name: "peerdb_record"}
	LuaRow     = glua64.UserDataType[*model.RecordItems]{Name: "peerdb_row"}
	LuaTime    = glua64.UserDataType[time.Time]{Name: "peerdb_time"}
	LuaUuid    = glua64.UserDataType[uuid.UUID]{Name: "peerdb_uuid"}
	LuaBigInt  = glua64.UserDataType[*big.Int]{Name: "peerdb_bigint"}
	LuaDecimal = glua64.UserDataType[decimal.Decimal]{Name: "peerdb_bigrat"}
)

func RegisterTypes(ls *lua.LState) {
	glua64.Loader(ls)
	ls.Env.RawSetString("loadfile", lua.LNil)
	ls.Env.RawSetString("dofile", lua.LNil)

	// gopher-lua provides 2 loaders {preload, file}
	// overwrite file loader with one retrieving scripts from database
	loaders := ls.G.Registry.RawGetString("_LOADERS").(*lua.LTable)
	loaders.RawSetInt(2, ls.NewFunction(LoadPeerdbScript))

	ls.PreloadModule("bit32", bit32.Loader)

	mt := LuaRecord.NewMetatable(ls)
	mt.RawSetString("__index", ls.NewFunction(LuaRecordIndex))

	mt = LuaRow.NewMetatable(ls)
	mt.RawSetString("__index", ls.NewFunction(LuaRowIndex))
	mt.RawSetString("__len", ls.NewFunction(LuaRowLen))

	mt = LuaUuid.NewMetatable(ls)
	mt.RawSetString("__index", ls.NewFunction(LuaUuidIndex))
	mt.RawSetString("__tostring", ls.NewFunction(LuaUuidString))

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
	peerdb.RawSetString("RowColumnKind", ls.NewFunction(LuaRowColumnKind))
	peerdb.RawSetString("Now", ls.NewFunction(LuaNow))
	peerdb.RawSetString("UUID", ls.NewFunction(LuaUUID))
	peerdb.RawSetString("type", ls.NewFunction(LuaType))
	peerdb.RawSetString("tostring", ls.NewFunction(LuaToString))
	ls.Env.RawSetString("peerdb", peerdb)
}

func LoadPeerdbScript(ls *lua.LState) int {
	ctx := ls.Context()
	name := ls.CheckString(1)
	pool, err := peerdbenv.GetCatalogConnectionPoolFromEnv(ctx)
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

func GetRowQ(ls *lua.LState, row *model.RecordItems, col string) qvalue.QValue {
	qv, err := row.GetValueByColName(col)
	if err != nil {
		ls.RaiseError(err.Error())
		return qvalue.QValue{}
	}
	return qv
}

func LuaRowIndex(ls *lua.LState) int {
	row, key := LuaRow.StartIndex(ls)
	ls.Push(LuaQValue(ls, GetRowQ(ls, row, key)))
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
		tbl.RawSetInt(idx+1, lua.LString(col))
	}
	ls.Push(tbl)
	return 1
}

func LuaRowColumnKind(ls *lua.LState) int {
	row, key := LuaRow.StartIndex(ls)
	ls.Push(lua.LString(GetRowQ(ls, row, key).Kind))
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
		ls.Push(glua64.I64.New(ls, record.GetCheckpointID()))
	case "commit_time":
		ls.Push(LuaTime.New(ls, record.GetCommitTime()))
	case "target":
		ls.Push(lua.LString(record.GetDestinationTableName()))
	case "source":
		ls.Push(lua.LString(record.GetSourceTableName()))
	default:
		return 0
	}
	return 1
}

func qvToLTable[T any](ls *lua.LState, s []T, f func(x T) lua.LValue) *lua.LTable {
	tbl := ls.CreateTable(len(s), 0)
	for idx, val := range s {
		tbl.RawSetInt(idx+1, f(val))
	}
	return tbl
}

func LuaQValue(ls *lua.LState, qv qvalue.QValue) lua.LValue {
	switch v := qv.Value.(type) {
	case nil:
		return lua.LNil
	case bool:
		return lua.LBool(v)
	case uint8:
		if qv.Kind == qvalue.QValueKindQChar {
			return lua.LString(rune(v))
		} else {
			return lua.LNumber(v)
		}
	case int16:
		return lua.LNumber(v)
	case int32:
		return lua.LNumber(v)
	case int64:
		return glua64.I64.New(ls, v)
	case float32:
		return lua.LNumber(v)
	case float64:
		return lua.LNumber(v)
	case string:
		if qv.Kind == qvalue.QValueKindUUID {
			u, err := uuid.Parse(v)
			if err != nil {
				return LuaUuid.New(ls, u)
			}
		}
		return lua.LString(v)
	case time.Time:
		return LuaTime.New(ls, v)
	case decimal.Decimal:
		return LuaDecimal.New(ls, v)
	case [16]byte:
		return LuaUuid.New(ls, uuid.UUID(v))
	case []byte:
		return lua.LString(v)
	case []float32:
		return qvToLTable(ls, v, func(f float32) lua.LValue {
			return lua.LNumber(f)
		})
	case []float64:
		return qvToLTable(ls, v, func(f float64) lua.LValue {
			return lua.LNumber(f)
		})
	case []int16:
		return qvToLTable(ls, v, func(x int16) lua.LValue {
			return lua.LNumber(x)
		})
	case []int32:
		return qvToLTable(ls, v, func(x int32) lua.LValue {
			return lua.LNumber(x)
		})
	case []int64:
		return qvToLTable(ls, v, func(x int64) lua.LValue {
			return glua64.I64.New(ls, x)
		})
	case []string:
		return qvToLTable(ls, v, func(x string) lua.LValue {
			return lua.LString(x)
		})
	case []time.Time:
		return qvToLTable(ls, v, func(x time.Time) lua.LValue {
			return LuaTime.New(ls, x)
		})
	case []bool:
		return qvToLTable(ls, v, func(x bool) lua.LValue {
			return lua.LBool(x)
		})
	default:
		return lua.LString(fmt.Sprint(qv.Value))
	}
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
	val := LuaUuid.StartMethod(ls)
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

func LuaType(ls *lua.LState) int {
	val := ls.Get(1)
	if ud, ok := val.(*lua.LUserData); ok {
		ls.Push(lua.LString(fmt.Sprintf("%T", ud.Value)))
		return 1
	}
	return 0
}

func LuaToString(ls *lua.LState) int {
	val := ls.Get(1)
	if ud, ok := val.(*lua.LUserData); ok {
		ls.Push(lua.LString(fmt.Sprint(ud.Value)))
		return 1
	}
	return 0
}

func LuaTimeIndex(ls *lua.LState) int {
	tm, key := LuaTime.StartIndex(ls)
	switch key {
	case "unix_nano":
		ls.Push(glua64.I64.New(ls, tm.UnixNano()))
	case "unix_micro":
		ls.Push(glua64.I64.New(ls, tm.UnixMicro()))
	case "unix_milli":
		ls.Push(glua64.I64.New(ls, tm.UnixMilli()))
	case "unix_second":
		ls.Push(glua64.I64.New(ls, tm.Unix()))
	case "unix":
		ls.Push(lua.LNumber(float64(tm.Unix()) + float64(tm.Nanosecond())/1e9))
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
	tm := LuaTime.StartMethod(ls)
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
			ls.Push(glua64.I64.New(ls, bi.Int64()))
		case "is64":
			ls.Push(lua.LBool(bi.IsInt64()))
		}
	default:
		ls.RaiseError("BigInt accessed with non number/string")
	}
	return 1
}

func LuaBigIntString(ls *lua.LState) int {
	bi := LuaBigInt.StartMethod(ls)
	ls.Push(lua.LString(bi.String()))
	return 1
}

func LuaBigIntLen(ls *lua.LState) int {
	bi := LuaBigInt.StartMethod(ls)
	ls.Push(lua.LNumber(len(bi.Bytes())))
	return 1
}

func LuaDecimalIndex(ls *lua.LState) int {
	num, key := LuaDecimal.StartIndex(ls)
	switch key {
	case "coefficient":
		ls.Push(LuaBigInt.New(ls, num.Coefficient()))
	case "coefficient64":
		ls.Push(glua64.I64.New(ls, num.CoefficientInt64()))
	case "exponent":
		ls.Push(lua.LNumber(num.Exponent()))
	case "bigint":
		ls.Push(LuaBigInt.New(ls, num.BigInt()))
	case "int64":
		ls.Push(glua64.I64.New(ls, num.IntPart()))
	case "float64":
		ls.Push(lua.LNumber(num.InexactFloat64()))
	default:
		return 0
	}
	return 1
}

func LuaDecimalString(ls *lua.LState) int {
	num := LuaDecimal.StartMethod(ls)
	ls.Push(lua.LString(num.String()))
	return 1
}
