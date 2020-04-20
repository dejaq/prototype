package errors

import (
	"fmt"
	"unsafe"
)

// MessageIDTuple a pair of MsgID and an error
type MessageIDTuple struct {
	MsgID    []byte
	MsgError Dejaror
}

func (t MessageIDTuple) Error() string {
	if len(t.MsgID) == 0 {
		return "<nil>"
	}
	return fmt.Sprintf("MessageIDTuple %s failed: %s", *(*string)(unsafe.Pointer(&t.MsgID)), t.MsgError.Message)
}

type MessageIDTupleList []MessageIDTuple

func (m MessageIDTupleList) Error() string {
	return fmt.Sprintf("MessageIDTupleList size %d", len(m))
}
