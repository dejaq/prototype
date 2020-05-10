package server

import (
	"math/rand"

	flatbuffers "github.com/google/flatbuffers/go"
)

// This is used for messages but also for other data types
type Msg struct {
	// For message the  Key is a 64bit (8 bytes) that contains 2 bytes priority + random bytes
	Key []byte
	// For Message is the body
	Val []byte
}

func (m Msg) GetKeyAsUint64() uint64 {
	if len(m.Key) != 8 {
		panic("corrupted message not 8bytes!")
	}
	return flatbuffers.GetUint64(m.Key)
}

func UInt64ToBytes(n uint64) []byte {
	result := make([]byte, 8)
	flatbuffers.WriteUint64(result, n)
	return result
}

func UInt16ToBytes(partition uint16) []byte {
	buf := make([]byte, 2)
	flatbuffers.WriteUint16(buf, partition)
	return buf
}
func generateMsgKey(priority uint16) []byte {
	priorityPrefix := UInt16ToBytes(priority)
	randomMsgId := make([]byte, 6)
	rand.Read(randomMsgId)
	return append(priorityPrefix, randomMsgId...)
}
