package pua

import (
	"github.com/yuin/gopher-lua"
)

const VtableMetadataFields int = 2

type Builder struct {
	ba        BinaryArray
	vtables   []int
	currentVT []int
	head      int
	objectEnd int
	finished  bool
	nested    bool
	minalign  uint8
}

func (b *Builder) EndVector(vectorSize int) int {
	if !b.nested {
		panic("EndVector called outside nested context")
	}
	b.PlaceU64(uint64(vectorSize), uint32n)
	return b.Offset()
}

func (b *Builder) Offset() int {
	return len(b.ba.data) - b.head
}

func (b *Builder) Pad(pad int) {
	if pad > 0 {
		b.head -= pad
		b.ba.Pad(pad, b.head)
	}
}

func (b *Builder) Place(ls *lua.LState, x lua.LValue, n N) {
	b.head -= int(n.width)
	n.Pack(ls, b.ba.data[b.head:], x)
}

func (b *Builder) PlaceU64(u64 uint64, n N) {
	b.head -= int(n.width)
	n.PackU64(b.ba.data[b.head:], u64)
}

func (b *Builder) Prep(width uint8, additional int) {
	if width > b.minalign {
		b.minalign = width
	}
	k := len(b.ba.data) - b.head + additional
	alignsize := (^k + 1) & int(width-1)
	desiredSize := alignsize + int(width) + additional

	for b.head < desiredSize {
		oldBufSize := len(b.ba.data)
		newBufSize := oldBufSize + 1
		for newBufSize < desiredSize {
			newBufSize *= 2
		}
		b.ba.Grow(newBufSize)
		b.head += len(b.ba.data) - oldBufSize
	}

	b.Pad(alignsize)
}

func (b *Builder) Prepend(ls *lua.LState, n N, x lua.LValue) {
	b.Prep(n.width, 0)
	b.Place(ls, x, n)
}

func (b *Builder) PrependU64(n N, x uint64) {
	b.Prep(n.width, 0)
	b.PlaceU64(x, n)
}

func (b *Builder) PrependSlot(ls *lua.LState, n N, slotnum int, x lua.LValue, d lua.LValue) {
	// TODO implement __eq for U64/I64
	if !ls.Equal(x, d) {
		b.Prepend(ls, n, x)
		b.Slot(slotnum)
	}
}

func (b *Builder) PrependOffsetTRelative(ls *lua.LState, off int, n N) {
	b.Prep(4, 0)
	boff := b.Offset()
	if off > boff {
		ls.RaiseError("Offset arithmetic error")
	} else {
		b.PlaceU64(uint64(boff-off+4), n)
	}
}

func (b *Builder) PrependSOffsetTRelative(ls *lua.LState, off int) {
	b.PrependOffsetTRelative(ls, off, int32n)
}

func (b *Builder) PrependUOffsetTRelative(ls *lua.LState, off int) {
	b.PrependOffsetTRelative(ls, off, uint32n)
}

func (b *Builder) PrependVOffsetT(off uint16) {
	b.Prep(2, 0)
	b.PlaceU64(uint64(off), uint16n)
}

func (b *Builder) Slot(slotnum int) {
	if !b.nested {
		panic("Slot called outside nested context")
	}
	for slotnum < len(b.currentVT) {
		b.currentVT = append(b.currentVT, 0)
	}
	b.currentVT[slotnum] = b.Offset()
}

func vtableEqual(a []int, objectStart int, b []byte) bool {
	if len(a)*2 != len(b) {
		return false
	}

	for i, ai := range a {
		x := int16n.UnpackU64(b[i*2:])
		if (x != 0 || ai != 0) && int(x) != objectStart-ai {
			return false
		}
	}
	return true
}

func (b *Builder) WriteVtable(ls *lua.LState) int {
	b.PrependSOffsetTRelative(ls, 0)
	objectOffset := b.Offset()

	for len(b.ba.data) > 0 && b.ba.data[len(b.ba.data)-1] == 0 {
		b.ba.data = b.ba.data[:len(b.ba.data)-1]
	}

	var existingVtable int
	for i := len(b.ba.data) - 1; i >= 0; i-- {
		vt2Offset := b.vtables[i]
		vt2Start := len(b.ba.data) - vt2Offset
		vt2Len := uint16n.UnpackU64(b.ba.data[vt2Start:])

		metadata := VtableMetadataFields * 2
		vt2End := vt2Start + int(vt2Len)
		vt2 := b.ba.data[vt2Start+metadata : vt2End]

		if vtableEqual(b.currentVT, objectOffset, vt2) {
			existingVtable = vt2Offset
			break
		}
	}

	if existingVtable == 0 {
		for i := len(b.currentVT) - 1; i >= 0; i-- {
			var off uint16
			if b.currentVT[i] != 0 {
				off = uint16(objectOffset - b.currentVT[i])
			}
			b.PrependVOffsetT(off)
		}

		objectSize := uint16(objectOffset - b.objectEnd)
		b.PrependVOffsetT(objectSize)

		vBytes := uint16(len(b.currentVT)+VtableMetadataFields) * 2
		b.PrependVOffsetT(vBytes)

		objectStart := len(b.ba.data) - objectOffset
		newOffset := b.Offset()
		int32n.PackU64(b.ba.data[newOffset-objectOffset:], uint64(objectStart))

		b.vtables = append(b.vtables, newOffset)
	} else {
		b.head = len(b.ba.data) - objectOffset
		int16n.PackU64(b.ba.data[b.head:], uint64(existingVtable-objectOffset))
	}

	if len(b.currentVT) != 0 {
		b.currentVT = b.currentVT[:0]
	}
	return objectOffset
}

var LuaBuilder = LuaUserDataType[*Builder]{Name: "flatbuffers_builder"}

func FlatBuffers_Builder_Loader(ls *lua.LState) int {
	m := ls.NewTable()
	ls.SetField(m, "New", ls.NewFunction(BuilderNew))

	mt := LuaBinaryArray.NewMetatable(ls)
	index := ls.SetFuncs(ls.NewTable(), map[string]lua.LGFunction{
		"Clear":              BuilderClear,
		"Output":             BuilderOutput,
		"StartObject":        BuilderStartObject,
		"WriteVtable":        BuilderWriteVtable,
		"EndObject":          BuilderEndObject,
		"Head":               BuilderHead,
		"Offset":             BuilderOffset,
		"Pad":                BuilderPad,
		"Prep":               BuilderPrep,
		"StartVector":        BuilderStartVector,
		"EndVector":          BuilderEndVector,
		"CreateString":       BuilderCreateString,
		"CreateByteVector":   BuilderCreateString,
		"Slot":               BuilderSlot,
		"Finish":             BuilderFinish,
		"FinishSizePrefixed": BuilderFinishSizePrefixed,
		"Place":              BuilderPlace,

		"PrependSlot":                 BuilderPrependSlot,
		"PrependBoolSlot":             BuilderPrependBoolSlot,
		"PrependByteSlot":             BuilderPrependUint8Slot,
		"PrependUint8Slot":            BuilderPrependUint8Slot,
		"PrependUint16Slot":           BuilderPrependUint16Slot,
		"PrependUint32Slot":           BuilderPrependUint32Slot,
		"PrependUint64Slot":           BuilderPrependUint64Slot,
		"PrependInt8Slot":             BuilderPrependInt8Slot,
		"PrependInt16Slot":            BuilderPrependInt16Slot,
		"PrependInt32Slot":            BuilderPrependInt32Slot,
		"PrependInt64Slot":            BuilderPrependInt64Slot,
		"PrependFloat32Slot":          BuilderPrependFloat32Slot,
		"PrependFloat64Slot":          BuilderPrependFloat64Slot,
		"PrependStructSlot":           BuilderPrependStructSlot,
		"PrependUOffsetTRelativeSlot": BuilderPrependUOffsetTRelativeSlot,

		"Prepend":                 BuilderPrepend,
		"PrependBool":             BuilderPrependBool,
		"PrependByte":             BuilderPrependUint8,
		"PrependUint8":            BuilderPrependUint8,
		"PrependUint16":           BuilderPrependUint16,
		"PrependUint32":           BuilderPrependUint32,
		"PrependUint64":           BuilderPrependUint64,
		"PrependInt8":             BuilderPrependInt8,
		"PrependInt16":            BuilderPrependInt16,
		"PrependInt32":            BuilderPrependInt32,
		"PrependInt64":            BuilderPrependInt64,
		"PrependFloat32":          BuilderPrependFloat32,
		"PrependFloat64":          BuilderPrependFloat64,
		"PrependVOffsetT":         BuilderPrependUint16,
		"PrependSOffsetTRelative": BuilderPrependSOffsetTRelative,
		"PrependUOffsetTRelative": BuilderPrependUOffsetTRelative,
	})
	ls.SetField(mt, "__index", index)

	ls.Push(m)
	return 1
}

func BuilderNew(ls *lua.LState) int {
	initialSize := int(ls.CheckNumber(1))

	ls.Push(LuaBuilder.New(ls, &Builder{
		ba:        BinaryArray{data: make([]byte, initialSize)},
		vtables:   nil,
		currentVT: nil,
		head:      initialSize,
		objectEnd: 0,
		finished:  false,
		nested:    false,
		minalign:  1,
	}))
	return 1
}

func BuilderClear(ls *lua.LState) int {
	b := LuaBuilder.StartMeta(ls)
	b.finished = false
	b.nested = false
	b.minalign = 1
	if len(b.vtables) != 0 {
		b.vtables = b.vtables[:0]
	}
	if len(b.currentVT) != 0 {
		b.currentVT = b.currentVT[:0]
	}
	b.objectEnd = 0
	b.head = len(b.ba.data)
	return 0
}

func BuilderOutput(ls *lua.LState) int {
	b := LuaBuilder.StartMeta(ls)
	if lua.LVIsFalse(ls.Get(2)) {
		ls.Push(lua.LString(b.ba.data[b.head:]))
	} else {
		ls.Push(lua.LString(b.ba.data))
	}
	return 1
}

func BuilderStartObject(ls *lua.LState) int {
	b := LuaBuilder.StartMeta(ls)
	if b.nested {
		ls.RaiseError("StartObject called inside nested context")
		return 0
	}

	numFields := int(ls.CheckNumber(2))
	b.currentVT = make([]int, numFields)
	b.objectEnd = b.Offset()
	b.nested = true
	return 0
}

func BuilderWriteVtable(ls *lua.LState) int {
	b := LuaBuilder.StartMeta(ls)
	b.WriteVtable(ls)
	return 0
}

func BuilderEndObject(ls *lua.LState) int {
	b := LuaBuilder.StartMeta(ls)
	if !b.nested {
		panic("EndObject called outside nested context")
	}
	ls.Push(lua.LNumber(b.WriteVtable(ls)))
	return 1
}

func BuilderHead(ls *lua.LState) int {
	b := LuaBuilder.StartMeta(ls)
	ls.Push(lua.LNumber(b.head))
	return 1
}

func BuilderOffset(ls *lua.LState) int {
	b := LuaBuilder.StartMeta(ls)
	ls.Push(lua.LNumber(len(b.ba.data) - b.head))
	return 1
}

func BuilderPad(ls *lua.LState) int {
	b := LuaBuilder.StartMeta(ls)
	pad := ls.CheckNumber(2)
	b.Pad(int(pad))
	return 0
}

func BuilderPrep(ls *lua.LState) int {
	b := LuaBuilder.StartMeta(ls)
	size := ls.CheckNumber(2)
	additional := ls.CheckNumber(3)
	b.Prep(uint8(size), int(additional))
	return 0
}

func BuilderPrependSOffsetTRelative(ls *lua.LState) int {
	b := LuaBuilder.StartMeta(ls)
	b.PrependOffsetTRelative(ls, int(ls.CheckNumber(2)), int32n)
	return 0
}

func BuilderPrependUOffsetTRelative(ls *lua.LState) int {
	b := LuaBuilder.StartMeta(ls)
	b.PrependOffsetTRelative(ls, int(ls.CheckNumber(2)), uint32n)
	return 0
}

func BuilderStartVector(ls *lua.LState) int {
	b := LuaBuilder.StartMeta(ls)
	if b.nested {
		ls.RaiseError("StartVector called in nested context")
	}
	b.nested = true
	elemSize := int(ls.CheckNumber(2))
	numElements := int(ls.CheckNumber(3))
	alignment := uint8(ls.CheckNumber(4))
	elementSize := elemSize * numElements
	b.Prep(4, elementSize)
	b.Prep(alignment, elementSize)
	ls.Push(lua.LNumber(b.Offset()))
	return 1
}

func BuilderEndVector(ls *lua.LState) int {
	b := LuaBuilder.StartMeta(ls)
	if !b.nested {
		ls.RaiseError("EndVector called outside nested context")
	}
	b.nested = false
	b.PlaceU64(uint64(ls.CheckNumber(2)), uint32n)
	ls.Push(lua.LNumber(b.Offset()))
	return 1
}

func BuilderCreateString(ls *lua.LState) int {
	b := LuaBuilder.StartMeta(ls)
	s := ls.CheckString(2)
	if b.nested {
		ls.RaiseError("CreateString called in nested context")
		return 0
	}
	b.nested = true

	lens := len(s)
	b.Prep(4, lens+1)
	b.PlaceU64(0, uint8n)
	b.head -= lens
	copy(b.ba.data[b.head:], s)

	return b.EndVector(lens)
}

func BuilderSlot(ls *lua.LState) int {
	b := LuaBuilder.StartMeta(ls)
	slotnum := int(ls.CheckNumber(2))
	b.Slot(slotnum)
	return 0
}

func FinishHelper(ls *lua.LState, sizePrefix bool) int {
	b := LuaBuilder.StartMeta(ls)
	rootTable := int(ls.CheckNumber(2))
	var additional int
	if sizePrefix {
		additional = 8
	} else {
		additional = 4
	}
	b.Prep(b.minalign, additional)
	b.PrependUOffsetTRelative(ls, rootTable)
	if sizePrefix {
		size := len(b.ba.data) - b.head
		b.PrependU64(int32n, uint64(size))
	}
	b.finished = true
	ls.Push(lua.LNumber(b.head))
	return 1
}

func BuilderFinish(ls *lua.LState) int {
	return FinishHelper(ls, false)
}

func BuilderFinishSizePrefixed(ls *lua.LState) int {
	return FinishHelper(ls, true)
}

func BuilderPlace(ls *lua.LState) int {
	b := LuaBuilder.StartMeta(ls)
	_, n := LuaN.Check(ls, 3)
	b.Place(ls, ls.Get(2), n)
	return 0
}

func BuilderPrependSlot(ls *lua.LState) int {
	b := LuaBuilder.StartMeta(ls)
	_, n := LuaN.Check(ls, 2)
	slotnum := int(ls.CheckNumber(3))
	b.PrependSlot(ls, n, slotnum, ls.Get(4), ls.Get(5))
	return 0
}

func PrependSlotHelper(ls *lua.LState, n N) int {
	b := LuaBuilder.StartMeta(ls)
	slotnum := int(ls.CheckNumber(2))
	b.PrependSlot(ls, n, slotnum, ls.Get(3), ls.Get(4))
	return 0
}

func BuilderPrependBoolSlot(ls *lua.LState) int {
	return PrependSlotHelper(ls, booln)
}

func BuilderPrependUint8Slot(ls *lua.LState) int {
	return PrependSlotHelper(ls, uint8n)
}

func BuilderPrependUint16Slot(ls *lua.LState) int {
	return PrependSlotHelper(ls, uint16n)
}

func BuilderPrependUint32Slot(ls *lua.LState) int {
	return PrependSlotHelper(ls, uint32n)
}

func BuilderPrependUint64Slot(ls *lua.LState) int {
	return PrependSlotHelper(ls, uint64n)
}

func BuilderPrependInt8Slot(ls *lua.LState) int {
	return PrependSlotHelper(ls, int8n)
}

func BuilderPrependInt16Slot(ls *lua.LState) int {
	return PrependSlotHelper(ls, int16n)
}

func BuilderPrependInt32Slot(ls *lua.LState) int {
	return PrependSlotHelper(ls, int32n)
}

func BuilderPrependInt64Slot(ls *lua.LState) int {
	return PrependSlotHelper(ls, int64n)
}

func BuilderPrependFloat32Slot(ls *lua.LState) int {
	return PrependSlotHelper(ls, float32n)
}

func BuilderPrependFloat64Slot(ls *lua.LState) int {
	return PrependSlotHelper(ls, float64n)
}

func BuilderPrependStructSlot(ls *lua.LState) int {
	b := LuaBuilder.StartMeta(ls)
	x := int(ls.CheckNumber(3))
	d := int(ls.CheckNumber(4))
	if x != d {
		if x != b.Offset() {
			ls.RaiseError("Tried to write a Struct at an Offset that is different from the current Offset of the Builder.")
		} else {
			b.Slot(int(ls.CheckNumber(2)))
		}
	}
	return 0
}

func BuilderPrependUOffsetTRelativeSlot(ls *lua.LState) int {
	b := LuaBuilder.StartMeta(ls)
	x := int(ls.CheckNumber(3))
	d := int(ls.CheckNumber(4))
	if x != d {
		b.PrependOffsetTRelative(ls, x, uint32n)
		b.Slot(int(ls.CheckNumber(2)))
	}
	return 0
}

func BuilderPrepend(ls *lua.LState) int {
	b := LuaBuilder.StartMeta(ls)
	_, n := LuaN.Check(ls, 2)
	b.Prepend(ls, n, ls.Get(3))
	return 0
}

func PrependHelper(ls *lua.LState, n N) int {
	b := LuaBuilder.StartMeta(ls)
	b.Prepend(ls, n, ls.Get(2))
	return 0
}

func BuilderPrependBool(ls *lua.LState) int {
	return PrependHelper(ls, booln)
}

func BuilderPrependUint8(ls *lua.LState) int {
	return PrependHelper(ls, uint8n)
}

func BuilderPrependUint16(ls *lua.LState) int {
	return PrependHelper(ls, uint16n)
}

func BuilderPrependUint32(ls *lua.LState) int {
	return PrependHelper(ls, uint32n)
}

func BuilderPrependUint64(ls *lua.LState) int {
	return PrependHelper(ls, uint64n)
}

func BuilderPrependInt8(ls *lua.LState) int {
	return PrependHelper(ls, int8n)
}

func BuilderPrependInt16(ls *lua.LState) int {
	return PrependHelper(ls, int16n)
}

func BuilderPrependInt32(ls *lua.LState) int {
	return PrependHelper(ls, int32n)
}

func BuilderPrependInt64(ls *lua.LState) int {
	return PrependHelper(ls, int64n)
}

func BuilderPrependFloat32(ls *lua.LState) int {
	return PrependHelper(ls, float32n)
}

func BuilderPrependFloat64(ls *lua.LState) int {
	return PrependHelper(ls, float64n)
}
