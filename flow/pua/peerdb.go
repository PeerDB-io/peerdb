package pua

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/yuin/gopher-lua"

	"github.com/PeerDB-io/peer-flow/connectors/utils/catalog"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

var (
	LuaRecord = LuaUserDataType[model.Record]{Name: "peerdb_record"}
	LuaRow    = LuaUserDataType[*model.RecordItems]{Name: "peerdb_row"}
	LuaQValue = LuaUserDataType[qvalue.QValue]{Name: "peerdb_value"}
)

func RegisterTypes(ls *lua.LState) {
	// gopher-lua provides 2 loaders {preload, file}
	// overwrite file loader with one retrieving scripts from database
	loaders := ls.GetField(ls.Get(lua.RegistryIndex), "_LOADERS").(*lua.LTable)
	ls.RawSetInt(loaders, 2, ls.NewFunction(LoadPeerdbScript))

	mt := LuaRecord.NewMetatable(ls)
	ls.SetField(mt, "__index", ls.NewFunction(LuaRecordIndex))

	mt = LuaRow.NewMetatable(ls)
	ls.SetField(mt, "__index", ls.NewFunction(LuaRowIndex))
	ls.SetField(mt, "__len", ls.NewFunction(LuaRowLen))

	mt = LuaQValue.NewMetatable(ls)
	ls.SetField(mt, "__index", ls.NewFunction(LuaQValueIndex))
	ls.SetField(mt, "__len", ls.NewFunction(LuaQValueLen))

	peerdb := ls.NewTable()
	ls.SetField(peerdb, "RowToJSON", ls.NewFunction(LuaRowToJSON))
	ls.SetField(peerdb, "RowColumns", ls.NewFunction(LuaRowColumns))
	ls.SetField(peerdb, "UnixNow", ls.NewFunction(LuaUnixNow))
	ls.SetGlobal("peerdb", peerdb)
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
	if err == nil {
		fn, err := ls.Load(bytes.NewReader(source), name)
		if err != nil {
			ls.RaiseError(err.Error())
		}
		ls.Push(fn)
	}
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
		ls.RawSetInt(tbl, idx, lua.LString(col))
	}
	ls.Push(tbl)
	return 1
}

func LuaRecordIndex(ls *lua.LState) int {
	record, key := LuaRecord.StartIndex(ls)
	switch key {
	case "kind":
		var tyname string
		switch record.(type) {
		case *model.InsertRecord:
			tyname = "insert"
		case *model.UpdateRecord:
			tyname = "update"
		case *model.DeleteRecord:
			tyname = "delete"
		case *model.RelationRecord:
			tyname = "relation"
		}
		ls.Push(lua.LString(tyname))
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
		ls.Push(LuaNI64.New(ls, NI64{record.GetCheckpointID()}))
	case "target":
		ls.Push(lua.LString(record.GetDestinationTableName()))
	case "source":
		ls.Push(lua.LString(record.GetSourceTableName()))
	default:
		return 0
	}
	return 1
}

func LuaQValueIndex(ls *lua.LState) int {
	qv, key := LuaQValue.StartIndex(ls)
	switch key {
	case "kind":
		ls.Push(lua.LString(qv.Kind))
	case "int64":
		ls.Push(LuaNI64.New(ls, NI64{reflect.ValueOf(qv.Value).Int()}))
	case "float64":
		ls.Push(lua.LNumber(reflect.ValueOf(qv.Value).Float()))
	case "string":
		ls.Push(lua.LString(fmt.Sprint(qv.Value)))
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

func LuaUnixNow(ls *lua.LState) int {
	ls.Push(lua.LNumber(float64(time.Now().UnixMilli()) / 1000.0))
	return 1
}
