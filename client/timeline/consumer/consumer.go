package consumer

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/dejaq/prototype/common/metrics/exporter"
	"github.com/prometheus/client_golang/prometheus"

	derror "github.com/dejaq/prototype/common/errors"
	dtime "github.com/dejaq/prototype/common/time"
	"github.com/dejaq/prototype/common/timeline"
	dejaq "github.com/dejaq/prototype/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

var (
	metricTopicLeasesCounter = exporter.GetConsumerCounter("topic_leases_count", []string{"operation", "topic", "consumer_id"})
	metricTopicLeasesErrors  = exporter.GetConsumerCounter("topic_leases_errors", []string{"operation", "topic", "consumer_id"})

	metricTopicMessagesCounter = exporter.GetConsumerCounter("topic_messages_count", []string{"operation", "topic", "consumer_id"})
	metricTopicMessagesErrors  = exporter.GetConsumerCounter("topic_messages_errors", []string{"operation", "topic", "consumer_id"})
)

var ErrMissingHandshake = errors.New("handshake is not node or session expired")
var ErrAlreadyRunning = errors.New("the consumer is already running")
var ErrNotStarted = errors.New("the consumer is not running")

type Config struct {
	ConsumerID             string
	Topic                  string
	Cluster                string
	MaxBufferSize          int64
	LeaseDuration          time.Duration
	UpdatePreloadStatsTick time.Duration
}

type Consumer struct {
	conf      *Config
	overseer  dejaq.BrokerClient
	carrier   dejaq.BrokerClient
	logger    logrus.FieldLogger
	msgBuffer chan timeline.Lease

	// protects all the mutable properties
	mutex     sync.RWMutex
	sessionID string

	currentStreamCtx    context.Context
	currentStreamCancel context.CancelFunc
}

func NewConsumer(overseer dejaq.BrokerClient, logger logrus.FieldLogger, carrier *grpc.ClientConn, conf *Config) *Consumer {
	result := &Consumer{
		conf:     conf,
		overseer: overseer,
		carrier:  dejaq.NewBrokerClient(carrier),
		logger: logger.WithFields(logrus.Fields{
			"component":  "consumer",
			"consumerID": conf.ConsumerID,
			"topic":      conf.Topic,
		}),
		msgBuffer: make(chan timeline.Lease, conf.MaxBufferSize),
	}

	return result
}

func (c *Consumer) Stop() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.currentStreamCancel != nil {
		c.currentStreamCancel()
	}
}

func (c *Consumer) IsRunning() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.currentStreamCtx != nil
}

func (c *Consumer) Start() error {
	if c.IsRunning() {
		return ErrAlreadyRunning
	}

	if c.getSessionID() == "" {
		return ErrMissingHandshake
	}

	err := c.openTheStream()

	if err != nil {
		//the session expired or the brokers lost it
		if strings.Contains(derror.ErrConsumerNotSubscribed.Error(), err.Error()) ||
			strings.Contains("session", err.Error()) {

			c.resetSession()
			err = ErrMissingHandshake
		}
	}
	return err
}

func (c *Consumer) ReadLease(ctx context.Context) (timeline.Lease, error) {
	if !c.IsRunning() {
		return timeline.Lease{}, ErrNotStarted
	}

	select {
	case <-ctx.Done():
		return timeline.Lease{}, context.Canceled
	case lease := <-c.msgBuffer:
		if lease.Message.TimestampMS < dtime.TimeToMS(time.Now()) {
			return lease, nil
		}
		msToWaitUntilIsAvailable := time.Duration(lease.Message.TimestampMS - dtime.TimeToMS(time.Now()))
		select {
		case <-ctx.Done():
			return timeline.Lease{}, context.Canceled
		case <-time.After(msToWaitUntilIsAvailable * time.Millisecond):
			return lease, nil
		}
	}
}

// Handshake has to be called before any carrier operation or when one
// fails with an invalid/expired session
func (c *Consumer) Handshake(ctx context.Context) error {
	if c.getSessionID() != "" {
		return nil
	}

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
	c.setSessionID(string(resp.SessionID()))
	return nil
}

func (c *Consumer) resetSession() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.sessionID = ""
}
func (c *Consumer) getSessionID() string {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.sessionID
}
func (c *Consumer) setSessionID(new string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.sessionID = new
}

func (c *Consumer) openTheStream() error {
	var builder *flatbuffers.Builder
	builder = flatbuffers.NewBuilder(128)

	sessionIDPosition := builder.CreateString(c.getSessionID())
	dejaq.TimelineConsumeRequestStart(builder)
	dejaq.TimelineConsumeRequestAddSessionID(builder, sessionIDPosition)
	requestPosition := dejaq.TimelineConsumeRequestEnd(builder)
	builder.Finish(requestPosition)

	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.currentStreamCtx, c.currentStreamCancel = context.WithCancel(context.Background())

	bidiStream, err := c.carrier.TimelineConsume(c.currentStreamCtx)
	if err != nil {
		c.currentStreamCancel()
		c.currentStreamCtx = nil
		c.currentStreamCancel = nil
		return errors.Wrap(err, "failed to open the bidiStream")
	}

	//these should close when the context is Done()
	go c.sendConsumerStatus(c.currentStreamCtx, bidiStream)
	go c.receiveMessages(c.currentStreamCtx, bidiStream)

	c.logger.Info("started consumer")
	return nil
}

// sendConsumerStatus keeps sending stats about this consumer, so the broker adjusts the amount of msgs it sends
func (c *Consumer) sendConsumerStatus(ctx context.Context, bidiStream dejaq.Broker_TimelineConsumeClient) {
	var builder *flatbuffers.Builder
	builder = flatbuffers.NewBuilder(128)
	ticker := time.NewTicker(c.conf.UpdatePreloadStatsTick)
	defer ticker.Stop() //otherwise it will leak
	sessionID := c.getSessionID()

	for {
		select {
		case <-ticker.C:
			builder.Reset()

			sessionIDPosition := builder.CreateString(sessionID)
			dejaq.TimelineConsumerStatusStart(builder)
			dejaq.TimelineConsumerStatusAddMaxBufferSize(builder, uint32(c.conf.MaxBufferSize))
			dejaq.TimelineConsumerStatusAddAvailableBufferSize(builder, uint32(int(c.conf.MaxBufferSize)-len(c.msgBuffer)))
			dejaq.TimelineConsumerStatusAddSessionID(builder, sessionIDPosition)
			dejaq.TimelineConsumerStatusAddTimeoutMS(builder, dtime.DurationToMS(c.conf.LeaseDuration))
			rootPosition := dejaq.TimelineConsumerStatusEnd(builder)
			builder.Finish(rootPosition)
			err := bidiStream.Send(builder)
			if err != nil {
				c.logger.WithError(err).Error("failed to update the openTheStream settings")
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *Consumer) receiveMessages(ctx context.Context, bidiStream dejaq.Broker_TimelineConsumeClient) {
	var err error
	var response *dejaq.TimelinePushLeaseResponse
	plabels := prometheus.Labels{"operation": "received", "topic": c.GetTopicID(), "consumer_id": c.conf.ConsumerID}

	for {
		if ctx.Err() != nil {
			break
		}
		//Recv is blocking
		response, err = bidiStream.Recv()
		if err == io.EOF { //it means the bidiStream batch is over
			break
		}
		if err != nil {
			//TODO find out why errors.Is is not working
			if !strings.Contains(err.Error(), context.Canceled.Error()) {
				metricTopicLeasesErrors.With(plabels).Inc()
				c.logger.WithError(err).Error("stream received an error")
			}
			break
		}
		if response == nil { //empty msg ?!?!?! TODO log this as a warning
			metricTopicLeasesErrors.With(plabels).Inc()
			c.logger.Error("empty response received from grpc in consumer")
			continue
		}

		msg := response.Message(nil)

		if msg == nil {
			metricTopicLeasesErrors.With(plabels).Inc()
			c.logger.Error("empty message received from grpc in consumer")
			continue
		}
		c.msgBuffer <- timeline.Lease{
			ExpirationTimestampMS: response.ExpirationTSMSUTC(),
			ConsumerID:            response.ConsumerIDBytes(),
			Message: timeline.MessageLease{
				ID:              msg.MessageIDBytes(),
				TimestampMS:     msg.TimestampMS(),
				ProducerGroupID: msg.ProducerGroupIDBytes(),
				Version:         msg.Version(),
				Body:            msg.BodyBytes(),
				BucketID:        msg.BucketID(),
			},
		}
		metricTopicLeasesCounter.With(plabels).Inc()
	}

	//terminate the session
	c.logger.Infof("stopping with err=%v", err)
	c.mutex.Lock()
	c.currentStreamCancel() //this also should close the upstream
	c.currentStreamCancel = nil
	c.currentStreamCtx = nil
	c.mutex.Unlock()

	if err != nil {
		if strings.Contains(derror.ErrConsumerNotSubscribed.Error(), err.Error()) ||
			strings.Contains("session", err.Error()) {
			c.resetSession()
		}
	}
}

func (c *Consumer) Delete(ctx context.Context, leases []timeline.Lease) error {
	sessionID := c.getSessionID()

	stream, err := c.carrier.TimelineDelete(ctx)
	plabels := prometheus.Labels{"operation": "delete", "topic": c.GetTopicID(), "consumer_id": c.conf.ConsumerID}
	if err != nil {
		metricTopicMessagesCounter.With(plabels).Inc()
		return fmt.Errorf("cannot create gRPC stream: %w", err)
	}

	var builder *flatbuffers.Builder
	builder = flatbuffers.NewBuilder(128)

	for i := range leases {
		builder.Reset()

		msgIDPosition := builder.CreateByteVector(leases[i].Message.ID)
		sessionIDPosition := builder.CreateString(sessionID)
		dejaq.TimelineDeleteRequestStart(builder)
		dejaq.TimelineDeleteRequestAddMessageID(builder, msgIDPosition)
		dejaq.TimelineDeleteRequestAddSessionID(builder, sessionIDPosition)
		dejaq.TimelineDeleteRequestAddBucketID(builder, leases[i].Message.BucketID)
		dejaq.TimelineDeleteRequestAddVersion(builder, leases[i].Message.Version)
		rootPosition := dejaq.TimelineDeleteRequestEnd(builder)
		builder.Finish(rootPosition)
		err = stream.Send(builder)
		if err != nil {
			metricTopicMessagesErrors.With(plabels).Inc()
			return fmt.Errorf("upstream failed: %w", err)
		}
	}

	response, err := stream.CloseAndRecv()
	if err != nil && err != io.EOF {
		metricTopicMessagesErrors.With(plabels).Inc()
		return err
	}

	err = derror.ParseTimelineResponse(response)
	if err != nil {
		metricTopicMessagesErrors.With(plabels).Inc()
		return err
	}
	metricTopicMessagesCounter.With(plabels).Add(float64(len(leases)))
	return nil
}

func (c *Consumer) GetConsumerID() string {
	return c.conf.ConsumerID
}

func (c *Consumer) GetTopicID() string {
	return c.conf.Topic
}
