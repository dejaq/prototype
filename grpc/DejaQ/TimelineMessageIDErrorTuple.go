// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package DejaQ

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type TimelineMessageIDErrorTuple struct {
	_tab flatbuffers.Table
}

func GetRootAsTimelineMessageIDErrorTuple(buf []byte, offset flatbuffers.UOffsetT) *TimelineMessageIDErrorTuple {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &TimelineMessageIDErrorTuple{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *TimelineMessageIDErrorTuple) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *TimelineMessageIDErrorTuple) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *TimelineMessageIDErrorTuple) MessgeID(j int) byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.GetByte(a + flatbuffers.UOffsetT(j*1))
	}
	return 0
}

func (rcv *TimelineMessageIDErrorTuple) MessgeIDLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *TimelineMessageIDErrorTuple) MessgeIDBytes() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *TimelineMessageIDErrorTuple) MutateMessgeID(j int, n byte) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.MutateByte(a+flatbuffers.UOffsetT(j*1), n)
	}
	return false
}

func (rcv *TimelineMessageIDErrorTuple) Err(obj *Error) *Error {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(Error)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func TimelineMessageIDErrorTupleStart(builder *flatbuffers.Builder) {
	builder.StartObject(2)
}
func TimelineMessageIDErrorTupleAddMessgeID(builder *flatbuffers.Builder, messgeID flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(messgeID), 0)
}
func TimelineMessageIDErrorTupleStartMessgeIDVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(1, numElems, 1)
}
func TimelineMessageIDErrorTupleAddErr(builder *flatbuffers.Builder, err flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(err), 0)
}
func TimelineMessageIDErrorTupleEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
