package client

import (
	"context"
	"errors"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/bgadrian/dejaq-broker/client/timeline/consumer"
	"github.com/bgadrian/dejaq-broker/client/timeline/producer"
	dejaq "github.com/bgadrian/dejaq-broker/grpc/DejaQ"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// Overseer is the gateway to access general commands
type Overseer interface {
	CreateTopic()
	DeleteTopic()
	TopicStatus()
	ClusterInfo()
}

// Client is the main window to the brokers
type Client interface {
	GetOverseerClient() Overseer
	NewConsumer(*consumer.Config) *consumer.Consumer
	NewProducer(*producer.Config) *producer.Producer
}

type Config struct {
	Cluster string
	Seeds   []string
}

var _ = Client(&Satellite{})

var ErrNoConnection = errors.New("no connections could be made to any broker seed")

// NewClient constructs a satellite. Cancel the provided context and all the connections, consumers and producers will close.
func NewClient(ctx context.Context, logger logrus.FieldLogger, conf *Config) (*Satellite, error) {
	result := &Satellite{
		baseCtx: ctx,
		logger:  logger,
		conf:    conf,
		conns:   []*grpc.ClientConn{},
	}

	for _, seed := range conf.Seeds {
		conn, err := grpc.Dial(seed, grpc.WithInsecure(), grpc.WithCodec(flatbuffers.FlatbuffersCodec{}))
		if err != nil || conn == nil {
			logger.WithError(err).Errorf("Failed to connect to: %s", seed)
			continue
		}
		result.conns = append(result.conns, conn)
	}

	if len(result.conns) == 0 {
		return nil, ErrNoConnection
	}

	//for _, conn := range result.conns {
	//	//TODO instantiate overseers here
	//	//result.overseers = append(result.overseers, )
	//}

	return result, nil
}

// Satellite is the default implementation of a general broker client
type Satellite struct {
	baseCtx   context.Context
	logger    logrus.FieldLogger
	conf      *Config
	conns     []*grpc.ClientConn
	overseers []dejaq.BrokerClient
}

func (s Satellite) GetOverseerClient() Overseer {
	panic("implement me")
}

func (s Satellite) NewConsumer(conf *consumer.Config) *consumer.Consumer {
	//TODO to find the carrier we have to call the overseer
	//with the topic, and get a list of overseers and send them
	//to the consumer
	return consumer.NewConsumer(s.conns[0], s.conns[0], conf)
}

func (s Satellite) NewProducer(conf *producer.Config) *producer.Producer {
	//TODO to find the carrier we have to call the overseer
	//with the topic, and get a list of overseers and send them
	//to the producer
	return producer.NewProducer(s.conns[0], s.conns[0], conf)
}
