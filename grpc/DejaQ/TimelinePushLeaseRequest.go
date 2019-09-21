// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package DejaQ

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type TimelinePushLeaseRequest struct {
	_tab flatbuffers.Table
}

func GetRootAsTimelinePushLeaseRequest(buf []byte, offset flatbuffers.UOffsetT) *TimelinePushLeaseRequest {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &TimelinePushLeaseRequest{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *TimelinePushLeaseRequest) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *TimelinePushLeaseRequest) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *TimelinePushLeaseRequest) ExpirationTSMSUTC() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *TimelinePushLeaseRequest) MutateExpirationTSMSUTC(n uint64) bool {
	return rcv._tab.MutateUint64Slot(4, n)
}

func (rcv *TimelinePushLeaseRequest) ConsumerID(j int) byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.GetByte(a + flatbuffers.UOffsetT(j*1))
	}
	return 0
}

func (rcv *TimelinePushLeaseRequest) ConsumerIDLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *TimelinePushLeaseRequest) ConsumerIDBytes() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *TimelinePushLeaseRequest) MutateConsumerID(j int, n byte) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.MutateByte(a+flatbuffers.UOffsetT(j*1), n)
	}
	return false
}

func (rcv *TimelinePushLeaseRequest) Message(obj *TimelinePushLeaseMessage) *TimelinePushLeaseMessage {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(TimelinePushLeaseMessage)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func TimelinePushLeaseRequestStart(builder *flatbuffers.Builder) {
	builder.StartObject(3)
}
func TimelinePushLeaseRequestAddExpirationTSMSUTC(builder *flatbuffers.Builder, expirationTSMSUTC uint64) {
	builder.PrependUint64Slot(0, expirationTSMSUTC, 0)
}
func TimelinePushLeaseRequestAddConsumerID(builder *flatbuffers.Builder, consumerID flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(consumerID), 0)
}
func TimelinePushLeaseRequestStartConsumerIDVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(1, numElems, 1)
}
func TimelinePushLeaseRequestAddMessage(builder *flatbuffers.Builder, message flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(2, flatbuffers.UOffsetT(message), 0)
}
func TimelinePushLeaseRequestEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
