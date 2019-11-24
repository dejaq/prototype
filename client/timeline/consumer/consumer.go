package consumer

import (
	"context"
	"io"
	"log"
	"sync"
	"time"

	dtime "github.com/bgadrian/dejaq-broker/common/time"
	"github.com/bgadrian/dejaq-broker/common/timeline"
	dejaq "github.com/bgadrian/dejaq-broker/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
	"google.golang.org/grpc"
)

type Config struct {
	ConsumerID             string
	Topic                  string
	Cluster                string
	MaxBufferSize          int64
	LeaseDuration          time.Duration
	ProcessMessageListener func(timeline.PushLeases)
}

type Consumer struct {
	conf           *Config
	overseer       dejaq.BrokerClient
	carrier        dejaq.BrokerClient
	sessionID      string
	msgBuffer      chan timeline.PushLeases
	handshakeMutex sync.RWMutex
}

func NewConsumer(overseer, carrier *grpc.ClientConn, conf *Config) *Consumer {
	result := &Consumer{
		conf:      conf,
		overseer:  dejaq.NewBrokerClient(overseer),
		carrier:   dejaq.NewBrokerClient(carrier),
		msgBuffer: make(chan timeline.PushLeases, conf.MaxBufferSize),
	}

	return result
}

func (c *Consumer) Start(ctx context.Context, f func(timeline.PushLeases)) {
	c.conf.ProcessMessageListener = f

	//TODO make this a proper method and see the goroutine doesn't leak
	go func() {
		if err := c.Handshake(ctx); err != nil {
			log.Println(err)
		}
		for {
			//now preload == process, TODO split it two
			c.preload(ctx)

			if ctx.Err() != nil {
				return
			}
		}
	}()
}

// Handshake has to be called before any carrier operation or when one
// fails with an invalid/expired session
func (c *Consumer) Handshake(ctx context.Context) error {
	c.handshakeMutex.Lock()
	defer c.handshakeMutex.Unlock()

	var builder *flatbuffers.Builder

	builder = flatbuffers.NewBuilder(128)
	clusterPos := builder.CreateString(c.conf.Cluster)
	consumerIDPos := builder.CreateString(c.conf.ConsumerID)
	topicIDPos := builder.CreateString(c.conf.Topic)
	dejaq.TimelineConsumerHandshakeRequestStart(builder)
	dejaq.TimelineConsumerHandshakeRequestAddCluster(builder, clusterPos)
	dejaq.TimelineConsumerHandshakeRequestAddConsumerID(builder, consumerIDPos)
	dejaq.TimelineConsumerHandshakeRequestAddTopicID(builder, topicIDPos)
	dejaq.TimelineConsumerHandshakeRequestAddLeaseTimeoutMS(builder, dtime.DurationToMS(c.conf.LeaseDuration))
	root := dejaq.TimelineConsumerHandshakeRequestEnd(builder)
	builder.Finish(root)

	resp, err := c.overseer.TimelineConsumerHandshake(ctx, builder)
	if err != nil {
		return err
	}
	c.sessionID = string(resp.SessionID())
	return nil
}

func (c *Consumer) preload(ctx context.Context) {
	c.handshakeMutex.RLock()
	defer c.handshakeMutex.RLock()

	var builder *flatbuffers.Builder
	builder = flatbuffers.NewBuilder(128)

	sessionIDPosition := builder.CreateString(c.sessionID)
	dejaq.TimelineConsumeRequestStart(builder)
	dejaq.TimelineConsumeRequestAddSessionID(builder, sessionIDPosition)
	requestPosition := dejaq.TimelineConsumeRequestEnd(builder)
	builder.Finish(requestPosition)

	stream, err := c.carrier.TimelineConsume(ctx, builder)
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	var response *dejaq.TimelinePushLeaseResponse
	go func() {
		for lease := range c.msgBuffer {
			if ctx.Err() != nil {
				break
			}
			if lease.Message.TimestampMS < dtime.TimeToMS(time.Now()) {
				c.conf.ProcessMessageListener(lease)
			} else {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Duration(lease.Message.TimestampMS-dtime.TimeToMS(time.Now())) * time.Millisecond):
					c.conf.ProcessMessageListener(lease)
				}
			}
		}
	}()
	for {
		err = nil

		if ctx.Err() != nil {
			break
		}
		for err == nil {
			//Recv is blocking
			response, err = stream.Recv()
			if err == io.EOF { //it means the stream batch is over
				break
			}
			if err != nil {
				if err != context.Canceled {
					log.Printf("consumer preload client failed err=%s", err.Error())
				}
				//TODO if err is invalid/expired sessionID do a handshake automatically
			}
			if response == nil { //empty msg ?!?!?! TODO log this as a warning
				continue
			}

			//TODO pass an object from a pool, to reuse it
			msg := response.Message(nil)
			c.msgBuffer <- timeline.PushLeases{
				ExpirationTimestampMS: response.ExpirationTSMSUTC(),
				ConsumerID:            response.ConsumerIDBytes(),
				Message: timeline.LeaseMessage{
					ID:              msg.MessageIDBytes(),
					TimestampMS:     msg.TimestampMS(),
					ProducerGroupID: msg.ProducerGroupIDBytes(),
					Version:         msg.Version(),
					Body:            msg.BodyBytes(),
					BucketID:        msg.BucketID(),
				},
			}
		}
	}
}

func (c *Consumer) Delete(ctx context.Context, msgs []timeline.Message) error {
	c.handshakeMutex.RLock()
	defer c.handshakeMutex.RLock()

	stream, err := c.carrier.TimelineDelete(ctx)
	if err != nil {
		log.Fatalf("Delete err: %s", err.Error())
	}

	var builder *flatbuffers.Builder
	builder = flatbuffers.NewBuilder(128)

	for i := range msgs {
		builder.Reset()

		msgIDPosition := builder.CreateByteVector(msgs[i].ID)
		sessionIDPosition := builder.CreateString(c.sessionID)
		dejaq.TimelineDeleteRequestStart(builder)
		dejaq.TimelineDeleteRequestAddMessageID(builder, msgIDPosition)
		dejaq.TimelineDeleteRequestAddSessionID(builder, sessionIDPosition)
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

func (c *Consumer) GetConsumerID() string {
	return c.conf.ConsumerID
}
