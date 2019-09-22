// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package DejaQ

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type Error struct {
	_tab flatbuffers.Table
}

func GetRootAsError(buf []byte, offset flatbuffers.UOffsetT) *Error {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Error{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *Error) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Error) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *Error) Severity() uint16 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetUint16(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Error) MutateSeverity(n uint16) bool {
	return rcv._tab.MutateUint16Slot(4, n)
}

func (rcv *Error) Module() byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.GetByte(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Error) MutateModule(n byte) bool {
	return rcv._tab.MutateByteSlot(6, n)
}

func (rcv *Error) Kind() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Error) MutateKind(n uint64) bool {
	return rcv._tab.MutateUint64Slot(8, n)
}

func (rcv *Error) Op() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *Error) Message() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *Error) Details(obj *ErrorDetails, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(14))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *Error) DetailsLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(14))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *Error) ThrottledMS() uint16 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(16))
	if o != 0 {
		return rcv._tab.GetUint16(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Error) MutateThrottledMS(n uint16) bool {
	return rcv._tab.MutateUint16Slot(16, n)
}

func (rcv *Error) ShouldSync() bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(18))
	if o != 0 {
		return rcv._tab.GetBool(o + rcv._tab.Pos)
	}
	return false
}

func (rcv *Error) MutateShouldSync(n bool) bool {
	return rcv._tab.MutateBoolSlot(18, n)
}

func (rcv *Error) ShouldRetry() bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(20))
	if o != 0 {
		return rcv._tab.GetBool(o + rcv._tab.Pos)
	}
	return false
}

func (rcv *Error) MutateShouldRetry(n bool) bool {
	return rcv._tab.MutateBoolSlot(20, n)
}

func ErrorStart(builder *flatbuffers.Builder) {
	builder.StartObject(9)
}
func ErrorAddSeverity(builder *flatbuffers.Builder, severity uint16) {
	builder.PrependUint16Slot(0, severity, 0)
}
func ErrorAddModule(builder *flatbuffers.Builder, module byte) {
	builder.PrependByteSlot(1, module, 0)
}
func ErrorAddKind(builder *flatbuffers.Builder, kind uint64) {
	builder.PrependUint64Slot(2, kind, 0)
}
func ErrorAddOp(builder *flatbuffers.Builder, op flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(3, flatbuffers.UOffsetT(op), 0)
}
func ErrorAddMessage(builder *flatbuffers.Builder, message flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(4, flatbuffers.UOffsetT(message), 0)
}
func ErrorAddDetails(builder *flatbuffers.Builder, details flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(5, flatbuffers.UOffsetT(details), 0)
}
func ErrorStartDetailsVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func ErrorAddThrottledMS(builder *flatbuffers.Builder, throttledMS uint16) {
	builder.PrependUint16Slot(6, throttledMS, 0)
}
func ErrorAddShouldSync(builder *flatbuffers.Builder, shouldSync bool) {
	builder.PrependBoolSlot(7, shouldSync, false)
}
func ErrorAddShouldRetry(builder *flatbuffers.Builder, shouldRetry bool) {
	builder.PrependBoolSlot(8, shouldRetry, false)
}
func ErrorEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
