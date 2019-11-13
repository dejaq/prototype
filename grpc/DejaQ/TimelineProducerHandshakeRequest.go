// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package DejaQ

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type TimelineProducerHandshakeRequest struct {
	_tab flatbuffers.Table
}

func GetRootAsTimelineProducerHandshakeRequest(buf []byte, offset flatbuffers.UOffsetT) *TimelineProducerHandshakeRequest {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &TimelineProducerHandshakeRequest{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *TimelineProducerHandshakeRequest) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *TimelineProducerHandshakeRequest) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *TimelineProducerHandshakeRequest) TraceID() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *TimelineProducerHandshakeRequest) TimeoutMS() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *TimelineProducerHandshakeRequest) MutateTimeoutMS(n uint64) bool {
	return rcv._tab.MutateUint64Slot(6, n)
}

func (rcv *TimelineProducerHandshakeRequest) ProducerGroupID() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *TimelineProducerHandshakeRequest) TopicID() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *TimelineProducerHandshakeRequest) Cluster() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *TimelineProducerHandshakeRequest) ProducerID() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(14))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func TimelineProducerHandshakeRequestStart(builder *flatbuffers.Builder) {
	builder.StartObject(6)
}
func TimelineProducerHandshakeRequestAddTraceID(builder *flatbuffers.Builder, traceID flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(traceID), 0)
}
func TimelineProducerHandshakeRequestAddTimeoutMS(builder *flatbuffers.Builder, timeoutMS uint64) {
	builder.PrependUint64Slot(1, timeoutMS, 0)
}
func TimelineProducerHandshakeRequestAddProducerGroupID(builder *flatbuffers.Builder, producerGroupID flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(2, flatbuffers.UOffsetT(producerGroupID), 0)
}
func TimelineProducerHandshakeRequestAddTopicID(builder *flatbuffers.Builder, topicID flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(3, flatbuffers.UOffsetT(topicID), 0)
}
func TimelineProducerHandshakeRequestAddCluster(builder *flatbuffers.Builder, cluster flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(4, flatbuffers.UOffsetT(cluster), 0)
}
func TimelineProducerHandshakeRequestAddProducerID(builder *flatbuffers.Builder, producerID flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(5, flatbuffers.UOffsetT(producerID), 0)
}
func TimelineProducerHandshakeRequestEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
