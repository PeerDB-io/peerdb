package utils

import (
	"context"
	"errors"
	"fmt"

	"github.com/yuin/gopher-lua"

	"github.com/PeerDB-io/gluaflatbuffers"
	"github.com/PeerDB-io/peer-flow/pua"
	"github.com/PeerDB-io/peer-flow/shared"
)

var errEmptyScript = errors.New("mirror must have script")

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
	if script == "" {
		return nil, errEmptyScript
	}

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
	err := ls.GPCall(pua.LoadPeerdbScript, lua.LString(script))
	if err != nil {
		return nil, fmt.Errorf("error loading script %s: %w", script, err)
	}
	err = ls.PCall(0, 0, nil)
	if err != nil {
		return nil, fmt.Errorf("error executing script %s: %w", script, err)
	}
	return ls, nil
}
