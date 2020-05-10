package pkg

import (
	"context"
	"fmt"
	"io"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/dejaq/prototype/grpc/DejaQ"
	"github.com/sirupsen/logrus"
)

type Message struct {
	id        uint64
	partition uint16
}

func Consume(ctx context.Context, consumerId int, logger logrus.FieldLogger, brokerClient DejaQ.BrokerClient) {
	logger.Info("Start consume")

	// get messages
	builder := flatbuffers.NewBuilder(0)

	for {
		if ctx.Err() != nil {
			return
		}

		builder.Reset()
		consumerOffset := builder.CreateByteVector([]byte(fmt.Sprintf("consumer_%d", consumerId)))
		DejaQ.ConsumerAskMessagesStart(builder)
		DejaQ.ConsumerAskMessagesAddNumber(builder, uint16(200))
		DejaQ.ConsumerAskMessagesAddConsumerId(builder, consumerOffset)
		root := DejaQ.ConsumerAskMessagesEnd(builder)
		builder.Finish(root)

		consumeStream, err := brokerClient.Consume(ctx, builder)
		if err != nil {
			logger.WithError(err).Fatal("consumer ask for messages")
		}

		var messages []Message
		for {
			req, err := consumeStream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				logger.WithError(err).Fatal("read from consumeStream error")
			}

			if req == nil {
				logger.Fatal("nil request")
			}

			messages = append(messages, Message{
				id:        req.Id(),
				partition: req.Partition(),
			})
		}

		ackStream, err := brokerClient.Acknowledge(ctx)
		if err != nil {
			logger.WithError(err).Fatal("consumer acknowledge")
		}

		for _, message := range messages {
			builder.Reset()
			DejaQ.AcknowledgeMessagesStart(builder)
			DejaQ.AcknowledgeMessagesAddId(builder, message.id)
			DejaQ.AcknowledgeMessagesAddPartition(builder, message.partition)
			root := DejaQ.AcknowledgeMessagesEnd(builder)

			builder.Finish(root)
			err := ackStream.Send(builder)
			if err != nil {
				logger.WithError(err).Error("consumer failed to send ack on stream")
				continue
			}
		}

		//ackResp, err := ackStream.CloseAndRecv()
		//if err != nil {
		//	logger.WithError(err).Error("send ack to broker")
		//	continue
		//}
		//
		//if !ackResp.Ack() {
		//	// TODO retry here, do something
		//	logger.Error("could not delete all messages from broker")
		//	continue
		//}

		logger.Info(fmt.Sprintf("delete messages: %d", len(messages)))
		<-time.After(1 * time.Second)
	}
}
