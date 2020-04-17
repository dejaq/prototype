package satellite

import (
	"context"
	"errors"

	"google.golang.org/grpc/connectivity"

	brokerClient "github.com/dejaq/prototype/client"
	chief "github.com/dejaq/prototype/client/overseer"
	"github.com/dejaq/prototype/client/timeline/consumer"
	"github.com/dejaq/prototype/client/timeline/producer"
	dejaq "github.com/dejaq/prototype/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type Config struct {
	Cluster        string
	OverseersSeeds []string
}

var _ = brokerClient.Client(&Satellite{})

var ErrNoConnection = errors.New("no connections could be made to any broker seed")

// NewClient constructs a satellite. Cancel the provided context and all the connections, consumers and producers will close.
func NewClient(ctx context.Context, logger logrus.FieldLogger, conf *Config) (*Satellite, error) {
	result := &Satellite{
		logger: logger,
		conf:   conf,
		conns:  []*grpc.ClientConn{},
	}
	result.baseCtx, result.closeEverything = context.WithCancel(ctx)

	for _, seed := range conf.OverseersSeeds {
		conn, err := grpc.Dial(seed,
			grpc.WithInsecure(),
			grpc.WithCodec(flatbuffers.FlatbuffersCodec{}),
			grpc.WithReadBufferSize(64*1024),
			grpc.WithWriteBufferSize(64*1024))
		if err != nil || conn == nil {
			logger.WithError(err).Errorf("Failed to connect to: %s", seed)
			continue
		}
		result.conns = append(result.conns, conn)
	}

	if len(result.conns) == 0 {
		return nil, ErrNoConnection
	}

	for _, conn := range result.conns {
		result.overseers = append(result.overseers, dejaq.NewBrokerClient(conn))
	}

	return result, nil
}

// Satellite is the default implementation of a general broker client
type Satellite struct {
	baseCtx         context.Context
	closeEverything context.CancelFunc
	logger          logrus.FieldLogger
	conf            *Config
	conns           []*grpc.ClientConn
	overseers       []dejaq.BrokerClient
}

func (s *Satellite) NewOverseerClient() brokerClient.Overseer {
	return chief.New(s.overseers)
}

func (s *Satellite) NewConsumer(conf *consumer.Config) *consumer.Consumer {
	//TODO to find the carrier we have to call the overseer
	//with the topic, and get a list of overseers and send them
	//to the consumer
	return consumer.NewConsumer(s.overseers[0], s.logger, s.conns[0], conf)
}

func (s *Satellite) NewProducer(conf *producer.Config) *producer.Producer {
	//TODO to find the carrier we have to call the overseer
	//with the topic, and get a list of overseers and send them
	//to the producer
	return producer.NewProducer(s.overseers[0], s.conns[0], conf)
}

func (s *Satellite) Close() {
	s.closeEverything()
	for _, c := range s.conns {
		c.Close()
	}
}

// WaitForConnection is a blocking call, until at least one of the overseer connections
// are Ready. It returns false if the context timed out.
// Presumes that ctx has a timeout
func (s *Satellite) WaitForConnection(ctx context.Context) bool {
	//TODO wait for all connections and close when the first one finished
	return s.conns[0].WaitForStateChange(ctx, connectivity.Ready)
}
