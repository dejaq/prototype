package server

import "crypto/rand"

type Msg struct {
	Key []byte
	Val []byte
}

func prefixPriority(priority uint16) []byte {
	buf := make([]byte, 2)
	// little-endian uint16 into a byte slice.
	_ = buf[1] // Force one bounds check. See: golang.org/issue/14808
	buf[0] = byte(priority)
	buf[1] = byte(priority >> 8)
	return buf
}
func generateMsgKey(priority uint16) []byte {
	priorityPrefix := prefixPriority(priority)
	randomMsgId := make([]byte, 6)
	rand.Read(randomMsgId)
	return append(priorityPrefix, randomMsgId...)
}
