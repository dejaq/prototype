package server

import (
	"fmt"
	"io"

	"github.com/dejaq/prototype/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/sirupsen/logrus"
)

type DejaqGrpc struct {
	logger logrus.FieldLogger
	topic  *TopicLocalData
}

func NewGRPC(logger logrus.FieldLogger, topic *TopicLocalData) *DejaqGrpc {
	return &DejaqGrpc{
		logger: logger,
		topic:  topic,
	}
}

func (d DejaqGrpc) Produce(stream DejaQ.Broker_ProduceServer) error {
	var err error
	var request *DejaQ.ProduceRequest

	topicBatch := make(map[uint16][]Msg, 10)

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

		partitionID := d.topic.GetRandomPartition()
		topicBatch[partitionID] = append(topicBatch[partitionID], Msg{
			Key: generateMsgKey(partitionID),
			Val: request.BodyBytes(),
		})
	}

	if err == nil {
		for partitionID, batch := range topicBatch {
			storage, gerr := d.topic.GetPartitionStorage(partitionID)
			if gerr != nil {
				err = gerr
				break
			}
			err = storage.AddMsgs(batch)
			if err != nil {
				break
			}
		}
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
