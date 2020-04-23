package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/ilyakaznacheev/cleanenv"

	"github.com/dejaq/prototype/broker/pkg/carrier"
	"github.com/dejaq/prototype/broker/pkg/overseer"
	"github.com/dejaq/prototype/broker/pkg/storage/cockroach"
	"github.com/dejaq/prototype/broker/pkg/storage/inmemory"
	"github.com/dejaq/prototype/broker/pkg/storage/redis"
	storageTimeline "github.com/dejaq/prototype/broker/pkg/storage/timeline"
	"github.com/dejaq/prototype/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type Config struct {
	Host         string `env:"HOST" env-default:"127.0.0.1:9000"`
	OverseerSeed string `env:"OVERSEER" env-default:"localhost:9000"`
	StorageType  string `env:"STORAGE_TYPE" env-default:"memory"`
	StorageHost  string `env:"STORAGE_HOST"`

	TopicCount        int `env:"TOPIC_COUNT"`
	ConsumersPerTopic int `env:"CONSUMERS_PER_TOPIC"`
	ProducersPerTopic int `env:"PRODUCERS_PER_TOPIC"`

	TimeoutDuration string `env:"TIMEOUT" env-default:"3s"`
}

func (c *Config) durationTimeout() time.Duration {
	r, _ := time.ParseDuration(c.TimeoutDuration)
	return r
}

func (c *Config) IsValid() error {
	if c.StorageType == "redis" || c.StorageType == "cockroach" {
		if c.StorageHost == "" {
			return errors.New("you should set StorageHost")
		}
	}

	if c.TopicCount < 1 {
		return errors.New("TopicCount should be > 0")
	}
	if c.ConsumersPerTopic < 1 {
		return errors.New("ConsumersPerTopic should be > 0")
	}
	if c.ProducersPerTopic < 1 {
		return errors.New("ProducersPerTopic should be > 0")
	}
	if _, err := time.ParseDuration(c.TimeoutDuration); err != nil {
		return fmt.Errorf("timeout provided but wrong value %s", err.Error())
	}

	return nil
}

func main() {
	logger := logrus.New().WithField("component", "broker")

	c := &Config{}
	err := cleanenv.ReadEnv(c)
	if err != nil {
		logger.Fatal(err)
	}
	if err = c.IsValid(); err != nil {
		logger.Fatal(err)
	}

	ctx, shutdownEverything := context.WithCancel(context.Background())
	if c.TimeoutDuration != "" {
		ctx, _ = context.WithTimeout(ctx, c.durationTimeout())
	}

	catalog := overseer.NewCatalog()
	storage, err := NewStorage(ctx, c, catalog, logger)
	if err != nil {
		logger.WithError(err).Fatal("failed startBroker")
	}

	greeter := carrier.NewGreeter()
	lis, err := net.Listen("tcp", c.Host)
	if err != nil {
		logger.Fatalf("failed to listen: %w", err)
	}
	ser := grpc.NewServer(
		grpc.CustomCodec(flatbuffers.FlatbuffersCodec{}),
		grpc.ConnectionTimeout(time.Second*120),
		grpc.MaxConcurrentStreams(uint32(c.TopicCount*(c.ConsumersPerTopic+c.ProducersPerTopic))),
	)
	grpServer := carrier.NewGRPCServer(nil)
	coordinatorConfig := carrier.Config{}
	dealer := carrier.NewExclusiveDealer()
	supervisor := carrier.NewCoordinator(ctx, &coordinatorConfig, storage, catalog, greeter, dealer)
	supervisor.AttachToServer(grpServer)

	go func() {
		defer logger.Info("closing SERVER goroutine")
		DejaQ.RegisterBrokerServer(ser, grpServer)
		logger.Info("start grpc server")
		if err := ser.Serve(lis); err != nil {
			logger.WithError(err).Error("grpc server failed")
		}
		shutdownEverything()
	}()

	go func() {
		//we need to wait for kill signal
		shutdownSignal := make(chan os.Signal, 1)
		signal.Notify(shutdownSignal)
		logger.Info("Press CTRL-C or kill the process to stop the broker")
		// Block until any signal is received or context closed, because we do not have a Default branch
		select {
		case <-shutdownSignal:
			shutdownEverything() //propagate trough the context
			return
		case <-ctx.Done():
			return
		}
	}()

	//wait until someone closed the context, is either CTRL+C or the producer finished/crashed
	<-ctx.Done()
	logger.Info("shutting down ...")
	ser.GracefulStop()
}

func NewStorage(ctx context.Context, config *Config, catalog *overseer.Catalog, logger logrus.FieldLogger) (storageTimeline.Repository, error) {
	switch config.StorageType {
	case "memory":
		return inmemory.New(catalog), nil
	case "redis":
		return redis.New(config.StorageHost)
	case "cockroach":
		// @Adrian can we move those inside cockroach client ?
		db, err := sql.Open("postgres", config.StorageHost) //&binary_parameters
		if err != nil {
			return nil, err
		}
		go func() {
			select {
			case <-ctx.Done():
				db.Close()
			}
		}()
		return cockroach.New(db, logger), nil
	default:
		return nil, errors.New("unknown storage")
	}
}
