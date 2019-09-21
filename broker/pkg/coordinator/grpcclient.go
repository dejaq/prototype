package coordinator

import (
	"context"
	"log"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/bgadrian/dejaq-broker/common/timeline"
	dejaq "github.com/bgadrian/dejaq-broker/grpc/DejaQ"
	grpc "google.golang.org/grpc"
)

type GRPCClient struct {
	client dejaq.BrokerClient
}

func NewGRPCClient(cc *grpc.ClientConn) *GRPCClient {
	c := GRPCClient{
		client: dejaq.NewBrokerClient(cc),
	}
	//automatically subscribe to the topic for testing
	_, err := c.client.TimelinePushLeases(context.Background(), nil)
	if err != nil {
		log.Fatal(err)
	}
	return &c
}

func (c *GRPCClient) InsertMessages(ctx context.Context, msgs []timeline.Message) error {
	stream, err := c.client.TimelineCreateMessages(ctx)
	if err != nil {
		return err
	}
	var builder *flatbuffers.Builder

	builder = flatbuffers.NewBuilder(128)

	for i := range msgs {
		msg := msgs[i]
		dejaq.TimelineCreateMessageRequestStart(builder)
		dejaq.TimelineCreateMessageRequestAddId(builder, builder.CreateByteVector(msg.ID))
		dejaq.TimelineCreateMessageRequestAddTimeoutMS(builder, msg.TimestampMS)
		dejaq.TimelineCreateMessageRequestAddBody(builder, builder.CreateByteVector(msg.Body))
		dejaq.TimelineCreateMessageRequestEnd(builder)
	}

	err = stream.Send(builder)
	return err
}
