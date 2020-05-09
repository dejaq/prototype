// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package DejaQ

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type Ack struct {
	_tab flatbuffers.Table
}

func GetRootAsAck(buf []byte, offset flatbuffers.UOffsetT) *Ack {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Ack{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *Ack) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Ack) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *Ack) Id(j int) uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.GetUint64(a + flatbuffers.UOffsetT(j*8))
	}
	return 0
}

func (rcv *Ack) IdLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *Ack) MutateId(j int, n uint64) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.MutateUint64(a+flatbuffers.UOffsetT(j*8), n)
	}
	return false
}

func AckStart(builder *flatbuffers.Builder) {
	builder.StartObject(1)
}
func AckAddId(builder *flatbuffers.Builder, id flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(id), 0)
}
func AckStartIdVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(8, numElems, 8)
}
func AckEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
