package consumer

import (
	"context"
	"io"
	"log"
	"sync"
	"time"

	"github.com/bgadrian/dejaq-broker/common/timeline"
	dejaq "github.com/bgadrian/dejaq-broker/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
	"google.golang.org/grpc"
)

type Config struct {
	ConsumerID             string
	Topic                  string
	Cluster                string
	LeaseMs                uint64
	ProcessMessageListener func(timeline.PushLeases)
}

type Consumer struct {
	conf           *Config
	overseer       dejaq.BrokerClient
	carrier        dejaq.BrokerClient
	sessionID      string
	handshakeMutex sync.RWMutex
}

func NewConsumer(ctx context.Context, overseer, carrier *grpc.ClientConn, conf *Config) *Consumer {
	result := &Consumer{
		conf:     conf,
		overseer: dejaq.NewBrokerClient(overseer),
		carrier:  dejaq.NewBrokerClient(carrier),
	}

	//TODO make this a proper method and see the goroutine doesn't leak
	go func() {
		if err := result.Handshake(context.Background()); err != nil {
			log.Fatal(err)
		}
		for {
			//now preload == process, TODO split it two
			result.preload(context.Background())
			time.Sleep(time.Millisecond * 25)

			if ctx.Err() != nil {
				return
			}
		}
	}()

	return result
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
	dejaq.TimelineConsumerHandshakeRequestAddLeaseTimeoutMS(builder, c.conf.LeaseMs)
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
	for {
		err = nil

		for err == nil {
			//time.Sleep(time.Millisecond * 200)
			//Recv is blocking
			response, err = stream.Recv()
			if err == io.EOF { //it means the stream batch is over
				break
			}
			if err != nil {
				log.Fatalf("TimelineCreateMessages client failed err=%s", err.Error())
				//TODO if err is invalid/expired sessionID do a handshake automatically
			}
			if response == nil { //empty msg ?!?!?! TODO log this as a warning
				continue
			}

			//TODO pass an object from a pool, to reuse it
			msg := response.Message(nil)
			c.conf.ProcessMessageListener(timeline.PushLeases{
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
			go func() {
				err := c.Delete(ctx, []timeline.Message{{
					ID:          msg.MessageIDBytes(),
					TimestampMS: msg.TimestampMS(),
					//BodyID:          nil,
					//Body:            nil,
					//ProducerGroupID: nil,
					//LockConsumerID:  nil,
					BucketID: msg.BucketID(),
					Version:  msg.Version(),
				}})
				if err != nil {
					log.Println("delete failed", err)
				}
			}()
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
