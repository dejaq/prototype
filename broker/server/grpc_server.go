package server

import (
	"fmt"
	"io"
	"math/rand"
	"time"

	"go.uber.org/atomic"

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

func (d DejaqGrpc) Produce(stream DejaQ.Broker_ProduceServer) error {
	var err error
	var request *DejaQ.ProduceRequest

	topicBatch := make(map[uint16][]Msg, 10)
	d.logger.Info("producer connected")
	d.logger.Info("producer disconnected")

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
			Key: generateMsgKey(request.Priority()),
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

func (d DejaqGrpc) Consume(req DejaQ.Broker_ConsumeServer) error {
	sendMsgTicker := time.NewTicker(batchFlushInterval)
	defer sendMsgTicker.Stop()

	consumerUniqueID := fmt.Sprintf("consumer_%d", rand.Int())
	logger := d.logger.WithField("consumerID", consumerUniqueID)

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
	defer logger.Info("consumer disconnected")
	var ackReceived atomic.Bool

	//the 	defer sendMsgTicker.Stop() will close this goroutine
	go func() {
		builder := flatbuffers.NewBuilder(1024)
		for range sendMsgTicker.C {
			if req.Context().Err() != nil {
				break
			}

			if !ackReceived.Load() {
				// the ack did not came for the previous batch
				//we wait one more cycle
				continue
			}

			msgs := storage.GetOldestMsgs(batchSize)
			for _, msg := range msgs {
				builder.Reset()
				bodyOffset := builder.CreateByteVector(msg.Val)
				DejaQ.MessageStart(builder)
				DejaQ.MessageAddId(builder, msg.GetKeyAsUint64())
				DejaQ.MessageAddBody(builder, bodyOffset)
				root := DejaQ.MessageEnd(builder)

				builder.Finish(root)
				err := req.Send(builder)
				if err != nil {
					logger.WithError(err).Error("failed to send a msg to consumer")
				}
			}
			ackReceived.Store(false)
		}
	}()

	//ack pipeline from consumer
	for {
		ackBatch, err := req.Recv()
		if err != nil {
			break
		}

		idsToRemove := make([][]byte, 10)
		for i := 0; i < ackBatch.IdLength(); i++ {
			msgID := ackBatch.Id(i)
			idsToRemove = append(idsToRemove, MsgKeyFromUint64(msgID))
		}

		err = storage.RemoveMsgs(idsToRemove)
		if err != nil {
			logger.WithError(err).Error("failed to remove from DB")
		}
		ackReceived.Store(true)
	}

	return err
}
