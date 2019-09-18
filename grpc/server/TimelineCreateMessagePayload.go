// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package server

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type TimelineCreateMessagePayload struct {
	_tab flatbuffers.Table
}

func GetRootAsTimelineCreateMessagePayload(buf []byte, offset flatbuffers.UOffsetT) *TimelineCreateMessagePayload {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &TimelineCreateMessagePayload{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *TimelineCreateMessagePayload) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *TimelineCreateMessagePayload) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *TimelineCreateMessagePayload) TraceID() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *TimelineCreateMessagePayload) TimeoutMS() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *TimelineCreateMessagePayload) MutateTimeoutMS(n uint64) bool {
	return rcv._tab.MutateUint64Slot(6, n)
}

func (rcv *TimelineCreateMessagePayload) Id(j int) byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.GetByte(a + flatbuffers.UOffsetT(j*1))
	}
	return 0
}

func (rcv *TimelineCreateMessagePayload) IdLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *TimelineCreateMessagePayload) IdBytes() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *TimelineCreateMessagePayload) MutateId(j int, n byte) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.MutateByte(a+flatbuffers.UOffsetT(j*1), n)
	}
	return false
}

func (rcv *TimelineCreateMessagePayload) Tsmsutc() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *TimelineCreateMessagePayload) MutateTsmsutc(n uint64) bool {
	return rcv._tab.MutateUint64Slot(10, n)
}

func (rcv *TimelineCreateMessagePayload) Body(j int) byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.GetByte(a + flatbuffers.UOffsetT(j*1))
	}
	return 0
}

func (rcv *TimelineCreateMessagePayload) BodyLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *TimelineCreateMessagePayload) BodyBytes() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *TimelineCreateMessagePayload) MutateBody(j int, n byte) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.MutateByte(a+flatbuffers.UOffsetT(j*1), n)
	}
	return false
}

func TimelineCreateMessagePayloadStart(builder *flatbuffers.Builder) {
	builder.StartObject(5)
}
func TimelineCreateMessagePayloadAddTraceID(builder *flatbuffers.Builder, traceID flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(traceID), 0)
}
func TimelineCreateMessagePayloadAddTimeoutMS(builder *flatbuffers.Builder, timeoutMS uint64) {
	builder.PrependUint64Slot(1, timeoutMS, 0)
}
func TimelineCreateMessagePayloadAddId(builder *flatbuffers.Builder, id flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(2, flatbuffers.UOffsetT(id), 0)
}
func TimelineCreateMessagePayloadStartIdVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(1, numElems, 1)
}
func TimelineCreateMessagePayloadAddTsmsutc(builder *flatbuffers.Builder, tsmsutc uint64) {
	builder.PrependUint64Slot(3, tsmsutc, 0)
}
func TimelineCreateMessagePayloadAddBody(builder *flatbuffers.Builder, body flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(4, flatbuffers.UOffsetT(body), 0)
}
func TimelineCreateMessagePayloadStartBodyVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(1, numElems, 1)
}
func TimelineCreateMessagePayloadEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
