package server

import (
	"fmt"
	"io"

	"github.com/dejaq/prototype/grpc/DejaQ"
	"github.com/dgraph-io/badger/v2"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type DejaqGrpc struct {
	db     *badger.DB
	logger logrus.FieldLogger
	topic  *Metadata
}

func NewGRPC(db *badger.DB, logger logrus.FieldLogger, topic *Metadata) *DejaqGrpc {
	return &DejaqGrpc{
		db:     db,
		logger: logger,
		topic:  topic,
	}
}

func (d DejaqGrpc) Produce(stream DejaQ.Broker_ProduceServer) error {
	var err error
	var request *DejaQ.ProduceRequest

	batch := []Msg{}

	//gather all the messages from the client
	for err == nil {
		request, err = stream.Recv()
		if request == nil { //empty msg ?!?!?! TODO log this as a warning
			continue
		}
		if err == io.EOF { //it means the stream batch is over
			break
		}
		if err != nil {
			_ = fmt.Errorf("grpc server TimelineCreateMessages client failed err=%s", err.Error())
			break
		}

		batch = append(batch, Msg{
			Key: generateMsgKey(d.topic.GetRandomPartition()),
			Val: request.BodyBytes(),
		})
	}

	if err == nil {
		//write to DB
		wb := d.db.NewWriteBatch()
		defer wb.Cancel()

		for _, msg := range batch {
			err = wb.Set(msg.Key, msg.Val)
			if err != nil {
				err = errors.Wrap(err, "cannot write to DB batch")
				break
			}
		}
		wb.Flush()
	}

	//returns the response to the client
	var builder *flatbuffers.Builder
	builder = flatbuffers.NewBuilder(128)

	DejaQ.ProduceResponseStart(builder)
	if err == nil {
		DejaQ.ProduceResponseAddAck(builder, true)
	} else {
		d.logger.WithError(err).Error("failed to produce msg")
		DejaQ.ProduceResponseAddAck(builder, false)
	}
	root := DejaQ.ProduceResponseEnd(builder)
	builder.Finish(root)
	err = stream.SendMsg(builder)
	if err != nil {
		d.logger.WithError(err).Error("Produce failed")
	}

	return nil
}

func (d DejaqGrpc) Consume(DejaQ.Broker_ConsumeServer) error {
	panic("implement me")
}
