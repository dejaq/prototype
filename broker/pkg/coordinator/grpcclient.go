package coordinator

import (
	"context"
	"io"
	"log"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/bgadrian/dejaq-broker/common/timeline"
	dejaq "github.com/bgadrian/dejaq-broker/grpc/DejaQ"
	"google.golang.org/grpc"
)

type GRPCClient struct {
	client                 dejaq.BrokerClient
	ProcessMessageListener func(timeline.PushLeases)
	config                 ClientConsumer
}

type ClientConsumer struct {
	ConsumerID []byte
	Topic      string
	Cluster    string
	LeaseMs    uint64
}

func NewGRPCClient(cc *grpc.ClientConn, config ClientConsumer) *GRPCClient {
	c := GRPCClient{
		client: dejaq.NewBrokerClient(cc),
		config: config,
	}

	go func() {
		for {
			select {
			//automatically subscribe to the topic for testing
			case <-time.After(time.Second):
				c.subscribe(context.Background())
			}
		}
	}()

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

	consumerIDPosition := builder.CreateByteVector(c.config.ConsumerID)
	clusterIDPosition := builder.CreateString(c.config.Cluster)
	topicIDPosition := builder.CreateString(c.config.Topic)
	dejaq.TimelineConsumerHandshakeRequestStart(builder)
	dejaq.TimelineConsumerHandshakeRequestAddConsumerID(builder, consumerIDPosition)
	dejaq.TimelineConsumerHandshakeRequestAddCluster(builder, clusterIDPosition)
	dejaq.TimelineConsumerHandshakeRequestAddTopicID(builder, topicIDPosition)
	dejaq.TimelineConsumerHandshakeRequestAddLeaseTimeoutMS(builder, c.config.LeaseMs)
	requestPosition := dejaq.TimelineConsumerHandshakeRequestEnd(builder)
	builder.Finish(requestPosition)

	stream, err := c.client.TimelineConsumerHandshake(context.Background(), builder)
	if err != nil && err != io.EOF {
		log.Fatalf("subscribe: %v", err)
	}

	for {
		err = nil
		var response *dejaq.TimelinePushLeaseResponse

		for err == nil {
			//time.Sleep(time.Millisecond * 200)
			//Recv is blocking
			response, err = stream.Recv()
			if response == nil { //empty msg ?!?!?! TODO log this as a warning
				continue
			}
			if err == io.EOF { //it means the stream batch is over
				break
			}
			if err != nil {
				log.Fatalf("TimelineCreateMessages client failed err=%s", err.Error())
				//TODO resubscribe
			}
			//TODO pass an object from a pool, to reuse it
			msg := response.Message(nil)
			c.ProcessMessageListener(timeline.PushLeases{
				ExpirationTimestampMS: response.ExpirationTSMSUTC(),
				ConsumerID:            response.ConsumerIDBytes(),
				Message: timeline.LeaseMessage{
					ID:              msg.MessageIDBytes(),
					TimestampMS:     msg.TimestampMS(),
					ProducerGroupID: msg.ProducerGroupIDBytes(),
					Version:         msg.Version(),
					Body:            msg.BodyBytes(),
				},
			})
			go c.Delete([]timeline.Message{{
				ID:          msg.MessageIDBytes(),
				TimestampMS: msg.TimestampMS(),
				//BodyID:          nil,
				//Body:            nil,
				//ProducerGroupID: nil,
				//LockConsumerID:  nil,
				BucketID: msg.BucketID(),
				Version:  msg.Version(),
			}})
		}
	}
}

func (c *GRPCClient) Delete(msgs []timeline.Message) error {
	stream, err := c.client.TimelineDelete(context.Background())
	if err != nil {
		log.Fatalf("Delete err: %s", err.Error())
	}

	var builder *flatbuffers.Builder
	builder = flatbuffers.NewBuilder(128)

	for i := range msgs {
		builder.Reset()

		msgIDPosition := builder.CreateByteVector(msgs[i].ID)
		dejaq.TimelineDeleteRequestStart(builder)
		dejaq.TimelineDeleteRequestAddMessageID(builder, msgIDPosition)
		dejaq.TimelineDeleteRequestAddBucketID(builder, msgs[i].BucketID)
		dejaq.TimelineDeleteRequestAddVersion(builder, msgs[i].Version)
		rootPosition := dejaq.TimelineDeleteRequestEnd(builder)
		builder.Finish(rootPosition)
		err = stream.Send(builder)
		if err != nil {
			log.Fatalf("Delete2 err: %s", err.Error())
		}
	}

	_, err = stream.CloseAndRecv()
	if err != nil && err != io.EOF {
		log.Fatalf("Delete3 err: %s", err.Error())
	}
	//if response != nil && response.MessagesErrorsLength() > 0 {
	//	var errorTuple dejaq.TimelineMessageIDErrorTuple
	//	var errorInErrorTuple dejaq.Error
	//	for i := 0; i < response.MessagesErrorsLength(); i++ {
	//		response.MessagesErrors(&errorTuple, i)
	//		errorTuple.Err(&errorInErrorTuple)
	//		log.Printf("Delete response error id:%s err:%s", string(errorTuple.MessgeIDBytes()), errorInErrorTuple.Message())
	//	}
	//}
	return nil
}
