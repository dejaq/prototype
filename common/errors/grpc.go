package errors

import (
	dejaq "github.com/dejaq/prototype/grpc/DejaQ"
)

func GrpcErrTupleToTuple(gerr dejaq.TimelineMessageIDErrorTuple) MessageIDTuple {
	rerr := gerr.Err(nil)
	return MessageIDTuple{
		MsgID:    gerr.MessgeIDBytes(),
		MsgError: GrpcErroToDerror(rerr),
	}
}

func GrpcErrToError(rerr *dejaq.Error) error {
	//if we do not write an ErrObject TimelineCreate will panic, so we put empty errors :(
	if rerr == nil || len(rerr.Message()) == 0 {
		return nil
	}
	return GrpcErroToDerror(rerr)
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

func ParseTimelineResponse(response *dejaq.TimelineResponse) error {
	if response != nil {
		rerr := response.Err(nil)
		if rerr != nil {
			return GrpcErroToDerror(rerr)
		}

		if response.MessagesErrorsLength() > 0 {
			individualErrors := make(MessageIDTupleList, response.MessagesErrorsLength())
			for i := range individualErrors {
				var gerr dejaq.TimelineMessageIDErrorTuple
				response.MessagesErrors(&gerr, i)
				individualErrors[i] = GrpcErrTupleToTuple(gerr)
			}
			return individualErrors
		}
	}
	return nil
}
