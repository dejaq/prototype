package coordinator

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/bgadrian/dejaq-broker/common/timeline"
	dejaq "github.com/bgadrian/dejaq-broker/grpc/DejaQ"
	"google.golang.org/grpc"
)

type GRPCClient struct {
	client dejaq.BrokerClient
}

func NewGRPCClient(cc *grpc.ClientConn) *GRPCClient {
	c := GRPCClient{
		client: dejaq.NewBrokerClient(cc),
	}

	//automatically subscribe to the topic for testing
	go c.subscribe(context.Background())
	return &c
}

func (c *GRPCClient) InsertMessages(ctx context.Context, msgs []timeline.Message) error {
	stream, err := c.client.TimelineCreateMessages(ctx)
	if err != nil {
		return err
	}
	var builder *flatbuffers.Builder

	builder = flatbuffers.NewBuilder(128)
	var root flatbuffers.UOffsetT

	for i := range msgs {
		msg := msgs[i]
		idPosition := builder.CreateByteVector(msg.ID)
		bodyPosition := builder.CreateByteVector(msg.Body)
		dejaq.TimelineCreateMessageRequestStart(builder)
		dejaq.TimelineCreateMessageRequestAddId(builder, idPosition)
		dejaq.TimelineCreateMessageRequestAddTimeoutMS(builder, msg.TimestampMS)
		dejaq.TimelineCreateMessageRequestAddBody(builder, bodyPosition)
		root = dejaq.TimelineCreateMessageRequestEnd(builder)
	}

	builder.Finish(root)
	err = stream.Send(builder)
	return err
}

func (c *GRPCClient) subscribe(ctx context.Context) {
	var builder *flatbuffers.Builder
	builder = flatbuffers.NewBuilder(128)
	dejaq.TimelinePushLeaseSubscribeRequestStart(builder)
	requestPosition := dejaq.TimelinePushLeaseSubscribeRequestEnd(builder)
	builder.Finish(requestPosition)

	stream, err := c.client.TimelinePushLeases(context.Background(), builder)
	if err != nil {
		log.Fatal(err)
	}

	for {
		time.Sleep(time.Second)
		err = nil
		var response *dejaq.TimelinePushLeaseResponse
		var msgs []timeline.Message

		for err == nil {
			//Recv is blocking
			response, err = stream.Recv()
			if response == nil { //empty msg ?!?!?! TODO log this as a warning
				continue
			}
			if err == io.EOF { //it means the stream batch is over
				break
			}
			if err != nil {
				fmt.Errorf("TimelineCreateMessages client failed err=%s", err.Error())
				//TODO resubscribe
				break
			}

			//TODO pass an object from a pool, to reuse it
			message := response.Message(nil)
			msgs = append(msgs, timeline.Message{
				ID:          message.MessageIDBytes(),
				TimestampMS: message.TimestampMS(),
				BodyID:      nil,
				Body:        message.BodyBytes(),
			})
		}

		//Process the messages
		for i := range msgs {
			fmt.Printf("received message ID=%s body=%s", msgs[i].GetID(), string(msgs[i].Body))
		}

		//DELETE them
		go c.Delete(msgs)
	}
}

func (c *GRPCClient) Delete(msgs []timeline.Message) error {
	stream, err := c.client.TimelineDelete(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	var builder *flatbuffers.Builder
	builder = flatbuffers.NewBuilder(128)

	for i := range msgs {
		dejaq.TimelineDeleteRequestStart(builder)
		dejaq.TimelineDeleteRequestAddMessageID(builder, builder.CreateByteVector(msgs[i].ID))
		dejaq.TimelineDeleteRequestEnd(builder)
		err = stream.Send(builder)
		if err != nil {
			log.Fatal(err)
		}
	}

	response, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatal(err)
	}
	var errorTuple dejaq.TimelineMessageIDErrorTuple
	var errorInErrorTuple dejaq.Error
	for i := 0; i < response.MessagesErrorsLength(); i++ {
		response.MessagesErrors(&errorTuple, i)
		errorTuple.Err(&errorInErrorTuple)
		log.Printf("Delete response error id:%s err:%s", string(errorTuple.MessgeIDBytes()), errorInErrorTuple.Message())
	}
	return nil
}
