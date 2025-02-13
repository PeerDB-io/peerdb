package utils

import (
	"context"
	"fmt"
	"strings"

	lua "github.com/yuin/gopher-lua"

	"github.com/PeerDB-io/gluajson"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pua"
	"github.com/PeerDB-io/peerdb/flow/shared"
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

func LuaPrintFn(fn func(string)) lua.LGFunction {
	return func(ls *lua.LState) int {
		top := ls.GetTop()
		ss := make([]string, top)
		for i := range top {
			ss[i] = ls.ToStringMeta(ls.Get(i + 1)).String()
		}
		fn(strings.Join(ss, "\t"))
		return 0
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
	switch record.(type) {
	case *model.InsertRecord[model.RecordItems],
		*model.UpdateRecord[model.RecordItems],
		*model.DeleteRecord[model.RecordItems]:
		ls.Push(ls.NewFunction(gluajson.LuaJsonEncode))
		ls.Push(ud)
		ls.Call(1, 1)
		return 1
	default:
		return 0
	}
}

type LPoolMessage[T any] struct {
	f   func(*lua.LState) T
	ret chan<- T
}
type LPool[T any] struct {
	messages chan LPoolMessage[T]
	returns  chan<- (<-chan T)
	wait     <-chan struct{}
	cons     func() (*lua.LState, error)
	maxSize  int
	size     int
	closed   bool
}

func LuaPool[T any](maxSize int, cons func() (*lua.LState, error), merge func(T)) (*LPool[T], error) {
	returns := make(chan (<-chan T), maxSize)
	wait := make(chan struct{})
	go func() {
		for ret := range returns {
			for val := range ret {
				merge(val)
			}
		}
		close(wait)
	}()

	pool := &LPool[T]{
		messages: make(chan LPoolMessage[T]),
		returns:  returns,
		wait:     wait,
		cons:     cons,
		maxSize:  maxSize,
		size:     0,
		closed:   false,
	}
	if err := pool.Spawn(); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func (pool *LPool[T]) Spawn() error {
	ls, err := pool.cons()
	if err != nil {
		return err
	}
	pool.size += 1
	go func() {
		defer ls.Close()
		for message := range pool.messages {
			message.ret <- message.f(ls)
			close(message.ret)
		}
	}()
	return nil
}

func (pool *LPool[T]) Close() {
	if !pool.closed {
		close(pool.returns)
		close(pool.messages)
		pool.closed = true
	}
}

func (pool *LPool[T]) Run(f func(*lua.LState) T) {
	ret := make(chan T, 1)
	msg := LPoolMessage[T]{f: f, ret: ret}
	if pool.size < pool.maxSize {
		select {
		case pool.messages <- msg:
			pool.returns <- ret
			return
		default:
			_ = pool.Spawn()
		}
	}
	pool.messages <- msg
	pool.returns <- ret
}

func (pool *LPool[T]) Wait(ctx context.Context) error {
	pool.Close()
	select {
	case <-pool.wait:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
