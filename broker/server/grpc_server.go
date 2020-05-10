package server

import (
	"fmt"
	"io"
	"time"

	"github.com/dejaq/prototype/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/sirupsen/logrus"
)

type DejaqGrpc struct {
	logger logrus.FieldLogger
	topic  *TopicLocalData

	consumerBatchSize          int
	consumerBatchFlushInterval time.Duration
}

func NewGRPC(logger logrus.FieldLogger, topic *TopicLocalData, consumerBatchSize int, consumerBatchFlushInterval time.Duration) *DejaqGrpc {
	return &DejaqGrpc{
		logger:                     logger,
		topic:                      topic,
		consumerBatchFlushInterval: consumerBatchFlushInterval,
		consumerBatchSize:          consumerBatchSize,
	}
}

func (d *DejaqGrpc) Produce(stream DejaQ.Broker_ProduceServer) error {
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
			// For PriorityQueues the Priority is only uint16
			Key: generateMsgKey(request.Priority()),
			Val: request.BodyBytes(),
		})
	}

	if err == nil || err == io.EOF {
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
	if err == nil || err == io.EOF {
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

func (d *DejaqGrpc) Consume(req *DejaQ.ConsumerAskMessages, resp DejaQ.Broker_ConsumeServer) error {
	consumerUniqueID := string(req.ConsumerId())
	logger := d.logger.WithField("consumerID", consumerUniqueID)

	err := d.topic.AddNewConsumer(consumerUniqueID)
	if err != nil {
		return err
	}
	logger.Infof("new consumer connected ID:%s", consumerUniqueID)

	consumerPartitionID, err := d.topic.GetPartitionForConsumer(consumerUniqueID)
	if err != nil {
		d.logger.WithError(err).Error("failed to get partition for consumer %s", consumerUniqueID)
		return err
	}

	logger = logger.WithField("partitionID", consumerPartitionID)
	logger.Info("assigned partition")
	defer d.topic.RemoveConsumer(consumerUniqueID)

	storage, err := d.topic.GetPartitionStorage(consumerPartitionID)
	if err != nil {
		logger.WithError(err).Error("failed to get partition storage")
		return err
	}

	builder := flatbuffers.NewBuilder(1024)
	msgs := storage.GetOldestMsgs(int(req.Number()))
	var successCount int
	for _, msg := range msgs {
		builder.Reset()
		bodyOffset := builder.CreateByteVector(msg.Val)
		DejaQ.MessageStart(builder)
		DejaQ.MessageAddId(builder, msg.GetKeyAsUint64())
		DejaQ.MessageAddPartition(builder, consumerPartitionID)
		DejaQ.MessageAddBody(builder, bodyOffset)
		root := DejaQ.MessageEnd(builder)

		builder.Finish(root)
		err := resp.Send(builder)
		if err != nil {
			logger.WithError(err).Error("failed to send a msg to consumer")
			continue
		}
		successCount++
	}

	d.logger.Info(fmt.Sprintf("Consumer: %s request: %d, and get: %d", consumerUniqueID, req.Number(), successCount))

	return err
}

func (d *DejaqGrpc) Acknowledge(req DejaQ.Broker_AcknowledgeServer) error {
	for {
		message, err := req.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			d.logger.WithError(err).Fatal("broker read ack stream")
		}

		storage, err := d.topic.GetPartitionStorage(message.Partition())
		if err != nil {
			d.logger.WithError(err).Error(" acknowledge failed to get partition storage")
			return err
		}

		idsToRemove := make([][]byte, 10)
		idsToRemove = append(idsToRemove, UInt64ToBytes(message.Id()))

		err = storage.RemoveMsgs(idsToRemove)
		if err != nil {
			d.logger.WithError(err).Error("failed to remove from DB")
		}
	}
	return nil
}
