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

	"github.com/dejaq/prototype/common/timeline"

	"github.com/dejaq/prototype/common/metrics/exporter"

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

const (
	subsystemBroker = "broker"
)

type Config struct {
	//default listens on all interfaces, this is standard for containers
	BindingAddress string `env:"BINDING_ADDRESS" env-default:"0.0.0.0:9000"`
	// memory, redis or cockroach/
	StorageType string `env:"STORAGE_TYPE" env-default:"memory"`
	// used to connect to redis or cockroach
	StorageHost string `env:"STORAGE_HOST"`

	//max no of messages for insert and delete
	CockroachMaxBatchSize int `env:"STORAGE_CRDB_MAXBATCH_COUNT" env-default:"10"`

	// max amount of concurrent GRPC streams
	MaxConnectionsLimit int `env:"CONNECTIONS_LIMIT" env-default:"1000"`
	// timeout for a GRPC idle connection
	ConnectionTimeoutDuration string `env:"CONNECTION_TIMEOUT" env-default:"120s"`

	// after this timeout the process will close automatically
	TimeoutDuration string `env:"TIMEOUT"`
	//max amount of leases to be fetched from the DB and sent to a consumer
	LoaderMaxBatchSize int `env:"LOADER_MAX_BATCH_SIZE" env-default:"100"`
}

func (c *Config) durationConnectionTimeout() time.Duration {
	r, _ := time.ParseDuration(c.ConnectionTimeoutDuration)
	return r
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

	if c.MaxConnectionsLimit < 1 {
		return errors.New("MaxConnectionsLimit should be > 0")
	}

	if _, err := time.ParseDuration(c.ConnectionTimeoutDuration); err != nil {
		return fmt.Errorf("connection timeout provided but wrong value %s", err.Error())
	}

	if c.TimeoutDuration != "" {
		if _, err := time.ParseDuration(c.TimeoutDuration); err != nil {
			return fmt.Errorf("timeout provided but wrong value %s", err.Error())
		}
	}

	if c.LoaderMaxBatchSize < 1 {
		c.LoaderMaxBatchSize = 100
	}
	if c.CockroachMaxBatchSize < 1 {
		c.CockroachMaxBatchSize = 10
	}
	return nil
}

func main() {
	go exporter.GetMetricsExporter(subsystemBroker, "2112")
	//go exporter.GetDefaultExporter("2113")

	logger := logrus.New().WithField("component", subsystemBroker)

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
	lis, err := net.Listen("tcp", c.BindingAddress)
	if err != nil {
		logger.Fatalf("failed to listen: %w", err)
	}
	ser := grpc.NewServer(
		grpc.CustomCodec(flatbuffers.FlatbuffersCodec{}),
		grpc.ConnectionTimeout(c.durationConnectionTimeout()),
		grpc.MaxConcurrentStreams(uint32(c.MaxConnectionsLimit)),
	)
	grpServer := carrier.NewGRPCServer(nil)
	coordinatorConfig := carrier.Config{LoaderMaxBatchSize: c.LoaderMaxBatchSize, AutoCreateTopicsSettings: &timeline.TopicSettings{
		BucketCount: uint16(1024),
	}}
	dealer := carrier.NewExclusiveDealer()
	supervisor := carrier.NewCoordinator(ctx, &coordinatorConfig, storage, catalog, greeter, dealer, logger)
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
		//TODO provision the user in dockercompose to avoid using root
		addr := fmt.Sprintf("postgresql://root@%s?sslmode=disable", config.StorageHost)
		db, err := sql.Open("postgres", addr) //&binary_parameters
		if err != nil {
			return nil, err
		}
		go func() {
			select {
			case <-ctx.Done():
				db.Close()
			}
		}()

		crClient := cockroach.New(db, logger)
		crClient.DeleteBatchMaxSize = config.CockroachMaxBatchSize
		crClient.InsertBatchMaxSize = config.CockroachMaxBatchSize
		return crClient, nil
	default:
		return nil, errors.New("unknown storage")
	}
}
