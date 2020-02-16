package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dejaq/prototype/client/timeline/producer"
	"github.com/dejaq/prototype/client/timeline/sync_produce"

	"github.com/spf13/viper"

	"github.com/dejaq/prototype/broker/pkg/coordinator"
	"github.com/dejaq/prototype/broker/pkg/overseer"
	"github.com/dejaq/prototype/broker/pkg/storage/cockroach"
	"github.com/dejaq/prototype/broker/pkg/storage/redis"
	storageTimeline "github.com/dejaq/prototype/broker/pkg/storage/timeline"
	brokerClient "github.com/dejaq/prototype/client"
	"github.com/dejaq/prototype/client/satellite"
	"github.com/dejaq/prototype/client/timeline/consumer"
	"github.com/dejaq/prototype/client/timeline/sync_consume"
	"github.com/dejaq/prototype/common/timeline"
	"github.com/dejaq/prototype/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/prometheus/common/log"
	"github.com/sirupsen/logrus"
	"go.uber.org/atomic"
	"google.golang.org/grpc"

	_ "github.com/lib/pq"
)

type Config struct {
	TopicCount          int    `mapstructure:"topic_count"`
	BatchSize           int    `mapstructure:"batch_size"`
	BucketCount         int    `mapstructure:"bucket_Count"`
	MessagesPerTopic    int    `mapstructure:"messages_per_topic"`
	ProducersPerTopic   int    `mapstructure:"producers_per_topic"`
	ConsumersPerTopic   int    `mapstructure:"consumers_per_topic"`
	ConsumersBufferSize int    `mapstructure:"consumers_buffer_size"`
	ProduceDeltaMinMS   int    `mapstructure:"produce_delta_min_ms"`
	ProduceDeltaMaxMS   int    `mapstructure:"produce_delta_max_ms"`
	OverseerSeedHost    string `mapstructure:"overseer_seed_host"`
	BrokerHost          string `mapstructure:"broker_host"`
	StorageType         string `mapstructure:"storage_type"`
	RedisHost           string `mapstructure:"redis_host"`
	RunTimeoutMS        int    `mapstructure:"run_timeout_ms"`
	StartProducers      bool   `mapstructure:"start_producers"`
	StartBroker         bool   `mapstructure:"start_broker"`
	StartConsumers      bool   `mapstructure:"start_consumers"`
	Seed                string `mapstructure:"seed"`
}

func main() {
	// load configuration
	cfg, err := loadConfig()
	if err != nil {
		panic("Can not read config file which is mandatory, provide the path to a 'config.yaml' as the first argument")
	}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond*time.Duration(cfg.RunTimeoutMS)))
	logger := logrus.New()

	if cfg.StartBroker {
		err = startBroker(ctx, cfg, logger, cancel)
		if err != nil {
			logger.WithError(err).Fatal("failed startBroker")
		}
	} else {
		logger.Info("skipping broker")
	}
	start := time.Now()
	defer func() {
		logger.Infof("finished in %dms", time.Now().Sub(start).Milliseconds())
	}()

	clientConfig := satellite.Config{
		Cluster:        "",
		OverseersSeeds: strings.Split(cfg.OverseerSeedHost, ","),
	}

	if cfg.Seed == "" {
		cfg.Seed = time.Now().UTC().Format("Jan_02_15_04_05")
	}

	wg := sync.WaitGroup{}
	for topicIndex := 0; topicIndex < cfg.TopicCount; topicIndex++ {
		//each topic has its own gRPC client, producers and consumers
		client, err := satellite.NewClient(ctx, logger, &clientConfig)
		if err != nil {
			logger.WithError(err).Fatal("brokerClient")
		}
		if !client.WaitForConnection(ctx) {
			logger.Error("The connection to the broker cannot be established in time.")
			return
		}
		chief := client.NewOverseerClient()
		defer func() {
			client.Close()
			//logger.Println("closing gRPC CLIENT goroutine")
		}()

		topicID := fmt.Sprintf("topic_%s_%d", cfg.Seed, topicIndex)
		wg.Add(1)
		go func(topic string) {
			defer wg.Done()

			testingParams := &deployConfig{
				producerGroupsCount: cfg.ProducersPerTopic,
				consumersCount:      cfg.ConsumersPerTopic,
				topic:               topic,
				msgsCount:           cfg.MessagesPerTopic,
				batchSize:           cfg.BatchSize,
				consumersBufferSize: int64(cfg.ConsumersBufferSize),
				produceDeltaMin:     time.Duration(cfg.ProduceDeltaMinMS) * time.Millisecond,
				produceDeltaMax:     time.Duration(cfg.ProduceDeltaMaxMS) * time.Millisecond,
			}

			if cfg.StartBroker {
				//even if we only start the broker, we create here the topics, this way
				//producers and consumers can start in any order they want, as separate processes.
				err := chief.CreateTimelineTopic(ctx, topic, timeline.TopicSettings{
					ReplicaCount:            0,
					MaxSecondsFutureAllowed: 10,
					MaxSecondsLease:         10,
					ChecksumBodies:          false,
					MaxBodySizeBytes:        100000,
					RQSLimitPerClient:       100000,
					MinimumProtocolVersion:  0,
					MinimumDriverVersion:    0,
					BucketCount:             uint16(cfg.BucketCount),
				})
				if err != nil {
					logger.WithError(err).Fatal("failed creating topic")
					return
				}
			}

			time.Sleep(time.Second)

			if cfg.StartProducers {
				runProducers(ctx, client, logger, testingParams)
			} else {
				logger.Info("skipping producers")
			}
			if cfg.StartConsumers {
				runConsumers(ctx, client, logger, testingParams)
			} else {
				logger.Info("skipping consumers")
			}
		}(topicID)
	}

	wg.Wait()

	//if this processes does not start all 3 components, we'll wait for a CTRL-C
	if cfg.StartBroker && (!cfg.StartProducers || !cfg.StartConsumers) {
		//we need to wait for kill signal
		c := make(chan os.Signal, 1)
		signal.Notify(c)
		logger.Info("Press CTRL-C or kill the process to stop the broker")
		// Block until any signal is received.
		<-c
	}
	logger.Info("all topics finished, closing everything")
	cancel() //propagate trough the context
}

func loadConfig() (Config, error) {
	v := viper.New()
	path, _ := os.Getwd()
	v.AddConfigPath(path)
	v.SetConfigName("config")
	v.AutomaticEnv()

	// config file is mandatory, env vars will not be read
	// if they are not present in config file,
	var cfg Config
	err := v.ReadInConfig()
	if err != nil {
		return cfg, err
	}

	err = v.Unmarshal(&cfg)
	overrideConfigByCLIParams(&cfg)

	return cfg, err
}

func overrideConfigByCLIParams(cfg *Config) {
	s := reflect.ValueOf(cfg).Elem()
	for i := 0; i < s.NumField(); i++ {
		v := s.Field(i)
		tagName, ok := s.Type().Field(i).Tag.Lookup("mapstructure")
		if !ok && !s.Field(i).CanSet() {
			continue
		}
		overrideField(tagName, v)
	}
}

func overrideField(tagName string, v reflect.Value) {
	for _, arg := range os.Args[1:] {
		if !strings.HasPrefix(arg[2:], tagName) {
			continue
		}
		val := strings.Split(arg, "=")[1]
		switch v.Kind() {
		case reflect.Int:
			if int64val, err := strconv.ParseInt(val, 10, 64); err == nil {
				v.SetInt(int64val)
			}
		case reflect.String:
			v.SetString(val)
		}
	}
}

func startBroker(ctx context.Context, cfg Config, logger *logrus.Logger, stopEverything context.CancelFunc) error {
	var storageClient storageTimeline.Repository
	switch cfg.StorageType {
	case "redis":
		var err error
		storageClient, err = redis.New(cfg.RedisHost)
		if err != nil {
			return fmt.Errorf("failed to connect to redis server: %w", err)
		}
	case "cockroach":
		// Connect to the "bank" database.
		db, err := sql.Open("postgres", "postgresql://duser@localhost:26257/dejaq?sslmode=disable") //&binary_parameters
		if err != nil {
			return fmt.Errorf("error connecting to the database: %w", err)
		}
		storageClient = cockroach.New(db, logger)
		go func() {
			select {
			case <-ctx.Done():
				db.Close()
			}
		}()
	default:
		return errors.New("unknown storage")
	}
	greeter := coordinator.NewGreeter()
	lis, err := net.Listen("tcp", cfg.BrokerHost)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	ser := grpc.NewServer(grpc.CustomCodec(flatbuffers.FlatbuffersCodec{}))
	grpServer := coordinator.NewGRPCServer(nil)
	coordinatorConfig := coordinator.Config{}
	dealer := coordinator.NewExclusiveDealer()
	catalog := overseer.NewCatalog()
	supervisor := coordinator.NewCoordinator(ctx, &coordinatorConfig, storageClient, catalog, greeter, dealer)
	supervisor.AttachToServer(grpServer)
	go func() {
		defer logger.Println("closing SERVER goroutine")

		DejaQ.RegisterBrokerServer(ser, grpServer)
		log.Info("start server")
		if err := ser.Serve(lis); err != nil {
			logger.Printf("Failed to serve: %v", err)
		}
		stopEverything() //this fix the case when the broker shutdowns by its own (not killed by the user or err)
	}()

	go func() {
		select {
		case <-ctx.Done():
			ser.GracefulStop()
		}
	}()
	return nil
}

type deployConfig struct {
	producerGroupsCount, consumersCount int
	topic                               string
	msgsCount, batchSize                int
	consumersBufferSize                 int64
	produceDeltaMin, produceDeltaMax    time.Duration
}

func runProducers(ctx context.Context, client brokerClient.Client, logger logrus.FieldLogger, config *deployConfig) {
	wgProducers := sync.WaitGroup{}
	leftToProduce := config.msgsCount
	aproxCountPerGroup := leftToProduce / config.producerGroupsCount

	for pi := 0; pi < config.producerGroupsCount; pi++ {
		wgProducers.Add(1)
		thisGroupShare := aproxCountPerGroup
		//if is the last one, get the rest of the messages
		if pi == config.producerGroupsCount-1 {
			thisGroupShare = leftToProduce
		}
		leftToProduce -= thisGroupShare

		go func(producerGroupID string, toProduce int) {
			defer wgProducers.Done()
			//TODO add more producers per group
			pc := sync_produce.SyncProduceConfig{
				Count:           toProduce,
				BatchSize:       config.batchSize,
				ProduceDeltaMin: config.produceDeltaMin,
				ProduceDeltaMax: config.produceDeltaMax,
				Producer: client.NewProducer(&producer.Config{
					Cluster:         "",
					Topic:           config.topic,
					ProducerGroupID: producerGroupID,
					ProducerID:      fmt.Sprintf("%s:%d", producerGroupID, rand.Int()),
				}),
			}

			err := sync_produce.Produce(ctx, &pc)
			if err != nil {
				log.Error(err)
			}
		}(fmt.Sprintf("producer_group_%d", pi), thisGroupShare)
	}
	wgProducers.Wait()
	logger.Infof("Successfully produced  %d messages on topic=%s", config.msgsCount, config.topic)
}

func runConsumers(ctx context.Context, client brokerClient.Client, logger logrus.FieldLogger, config *deployConfig) {
	msgCounter := new(atomic.Int64)
	msgCounter.Add(int64(config.msgsCount))
	consumersCtx, closeConsumers := context.WithCancel(ctx)

	for ci := 0; ci < config.consumersCount; ci++ {
		go func(consumerID string, counter *atomic.Int64) {
			cc := sync_consume.SyncConsumeConfig{
				Consumer: client.NewConsumer(&consumer.Config{
					ConsumerID:    consumerID,
					Topic:         config.topic,
					Cluster:       "",
					MaxBufferSize: config.consumersBufferSize,
					LeaseDuration: time.Millisecond * 1000,
				}),
			}
			avgFullRoundTripMS, err := sync_consume.Consume(consumersCtx, counter, &cc)
			if err != nil {
				log.Error(err)
			}
			//duration, _ := time.ParseDuration(fmt.Sprintf("%dms", avgFullRoundTripMS))
			logger.Infof("avg round trip was %dms", avgFullRoundTripMS)
		}(fmt.Sprintf("consumer_%d", ci), msgCounter)
	}

	checker := time.NewTicker(time.Millisecond * 10)
	defer checker.Stop()

	for {
		select {
		case <-checker.C:
			if msgCounter.Load() == 0 {
				closeConsumers()
				logger.Infof("Successfully produced and consumed %d messages on topic=%s", config.msgsCount, config.topic)
				return
			}
		case <-ctx.Done():
			logger.Errorf("Failed to consume all the produced messages, %d left on topic=%s", msgCounter.Load(), config.topic)
			return
		}
	}
}
