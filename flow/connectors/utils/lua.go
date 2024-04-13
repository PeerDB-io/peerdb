package utils

import (
	"context"
	"fmt"

	"github.com/jackc/puddle/v2"
	"github.com/yuin/gopher-lua"

	"github.com/PeerDB-io/gluaflatbuffers"
	"github.com/PeerDB-io/gluajson"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/pua"
	"github.com/PeerDB-io/peer-flow/shared"
)

func LVAsReadOnlyBytes(ls *lua.LState, v lua.LValue) ([]byte, error) {
	str, err := LVAsStringOrNil(ls, v)
	if err != nil {
		return nil, err
	} else if str == "" {
		return nil, nil
	} else {
		return shared.UnsafeFastStringToReadOnlyBytes(str), nil
	}
}

func LVAsStringOrNil(ls *lua.LState, v lua.LValue) (string, error) {
	if lstr, ok := v.(lua.LString); ok {
		return string(lstr), nil
	} else if v == lua.LNil {
		return "", nil
	} else {
		return "", fmt.Errorf("invalid bytes, must be nil or string: %s", v)
	}
}

func LoadScript(ctx context.Context, script string, printfn lua.LGFunction) (*lua.LState, error) {
	ls := lua.NewState(lua.Options{SkipOpenLibs: true})
	ls.SetContext(ctx)
	for _, pair := range []struct {
		f lua.LGFunction
		n string
	}{
		{lua.OpenPackage, lua.LoadLibName}, // Must be first
		{lua.OpenBase, lua.BaseLibName},
		{lua.OpenTable, lua.TabLibName},
		{lua.OpenString, lua.StringLibName},
		{lua.OpenMath, lua.MathLibName},
	} {
		ls.Push(ls.NewFunction(pair.f))
		ls.Push(lua.LString(pair.n))
		err := ls.PCall(1, 0, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize Lua runtime: %w", err)
		}
	}
	ls.PreloadModule("flatbuffers", gluaflatbuffers.Loader)
	pua.RegisterTypes(ls)
	ls.Env.RawSetString("print", ls.NewFunction(printfn))
	if script != "" {
		err := ls.GPCall(pua.LoadPeerdbScript, lua.LString(script))
		if err != nil {
			return nil, fmt.Errorf("error loading script %s: %w", script, err)
		}
		err = ls.PCall(0, 0, nil)
		if err != nil {
			return nil, fmt.Errorf("error executing script %s: %w", script, err)
		}
	}
	return ls, nil
}

func DefaultOnRecord(ls *lua.LState) int {
	ud, record := pua.LuaRecord.Check(ls, 1)
	if _, ok := record.(*model.RelationRecord[model.RecordItems]); ok {
		return 0
	}
	ls.Push(ls.NewFunction(gluajson.LuaJsonEncode))
	ls.Push(ud)
	ls.Call(1, 1)
	return 1
}

type LPool = *puddle.Pool[*lua.LState]

func LuaPool(cons puddle.Constructor[*lua.LState]) LPool {
	pool, err := puddle.NewPool(&puddle.Config[*lua.LState]{
		Constructor: cons,
		Destructor:  (*lua.LState).Close,
		MaxSize:     4, // TODO env variable
	})
	if err != nil {
		// only happens when MaxSize < 1
		panic(err)
	}
	return pool
}

func LuaPoolJob(ctx context.Context, pool LPool, wait <-chan struct{}, f func(*lua.LState, <-chan struct{})) (chan struct{}, error) {
	res, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	block := make(chan struct{})
	go func() {
		defer close(block)
		defer res.Release()
		f(res.Value(), wait)
	}()
	return block, nil
}
