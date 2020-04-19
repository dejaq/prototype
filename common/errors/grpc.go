package errors

import (
	dejaq "github.com/dejaq/prototype/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
)

func GrpcErrTupleToTuple(gerr dejaq.TimelineMessageIDErrorTuple) MessageIDTuple {
	rerr := gerr.Err(nil)
	return MessageIDTuple{
		MsgID:    gerr.MessgeIDBytes(),
		MsgError: GrpcErroToDerror(rerr),
	}
}

func GrpcErroToDerror(rerr *dejaq.Error) Dejaror {
	return Dejaror{
		Severity:         Severity(rerr.Severity()),
		Message:          string(rerr.Message()),
		Module:           Module(rerr.Module()),
		Operation:        Op(rerr.Op()),
		Kind:             Kind(rerr.Kind()),
		Details:          nil,
		WrappedErr:       nil,
		ShouldRetry:      rerr.ShouldRetry(),
		ClientShouldSync: rerr.ShouldSync(),
	}
}

type GrpcTable interface {
	Table() flatbuffers.Table
}

// grpc returns pointers to structs that throws index out of range panics. Until I figure it out
// this replaces the nil check
func IsGrpcElementEmpty(fbElement GrpcTable) bool {
	return int(fbElement.Table().Pos) >= len(fbElement.Table().Bytes)
}
