// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package DejaQ

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type TimelineResponse struct {
	_tab flatbuffers.Table
}

func GetRootAsTimelineResponse(buf []byte, offset flatbuffers.UOffsetT) *TimelineResponse {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &TimelineResponse{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *TimelineResponse) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *TimelineResponse) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *TimelineResponse) Err(obj *Error) *Error {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
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

func (rcv *TimelineResponse) MessagesErrors(obj *TimelineMessageIDErrorTuple, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *TimelineResponse) MessagesErrorsLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func TimelineResponseStart(builder *flatbuffers.Builder) {
	builder.StartObject(2)
}
func TimelineResponseAddErr(builder *flatbuffers.Builder, err flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(err), 0)
}
func TimelineResponseAddMessagesErrors(builder *flatbuffers.Builder, messagesErrors flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(messagesErrors), 0)
}
func TimelineResponseStartMessagesErrorsVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func TimelineResponseEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}