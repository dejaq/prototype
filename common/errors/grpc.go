package errors

import dejaq "github.com/dejaq/prototype/grpc/DejaQ"

func GrpcErrTupleToTuple(gerr dejaq.TimelineMessageIDErrorTuple) MessageIDTuple {
	rerr := gerr.Err(nil)
	return MessageIDTuple{
		MsgID:    gerr.MessgeIDBytes(),
		MsgError: GrpcErroToDerror(rerr),
	}
}

func GrpcErroToDerror(rerr *dejaq.Error) Dejaror {
	if rerr == nil {
		return Dejaror{}
	}
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
