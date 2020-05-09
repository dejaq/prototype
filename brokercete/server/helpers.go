package server

import (
	"math/rand"

	flatbuffers "github.com/google/flatbuffers/go"
)

type Msg struct {
	Key []byte
	Val []byte
}

func (m Msg) GetKeyAsUint64() uint64 {
	if len(m.Key) != 8 {
		panic("corrupted message not 8bytes!")
	}
	return flatbuffers.GetUint64(m.Key)
}

func MsgKeyFromUint64(n uint64) []byte {
	result := make([]byte, 8)
	flatbuffers.WriteUint64(result, n)
	return result
}

func prefixPriority(priority uint16) []byte {
	buf := make([]byte, 2)
	flatbuffers.WriteUint16(buf, priority)
	return buf
}
func generateMsgKey(priority uint16) []byte {
	priorityPrefix := prefixPriority(priority)
	randomMsgId := make([]byte, 6)
	rand.Read(randomMsgId)
	return append(priorityPrefix, randomMsgId...)
}
