package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dejaq/prototype/broker/pkg/carrier"
	"github.com/dejaq/prototype/broker/pkg/overseer"
	"github.com/dejaq/prototype/broker/pkg/storage/cockroach"
	"github.com/dejaq/prototype/broker/pkg/storage/inmemory"
	"github.com/dejaq/prototype/broker/pkg/storage/redis"
	storageTimeline "github.com/dejaq/prototype/broker/pkg/storage/timeline"
	brokerClient "github.com/dejaq/prototype/client"
	"github.com/dejaq/prototype/client/satellite"
	"github.com/dejaq/prototype/client/timeline/consumer"
	"github.com/dejaq/prototype/client/timeline/producer"
	"github.com/dejaq/prototype/client/timeline/sync_consume"
	"github.com/dejaq/prototype/client/timeline/sync_produce"
	"github.com/dejaq/prototype/common/metrics/exporter"
	"github.com/dejaq/prototype/common/timeline"
	"github.com/dejaq/prototype/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
	_ "github.com/lib/pq"
	"github.com/prometheus/common/log"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
)

type Config struct {
	TopicCount          int    `mapstructure:"topic_count"`
	BatchSize           int    `mapstructure:"batch_size"`
	BucketCount         int    `mapstructure:"bucket_count"`
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
	InternalRunTimeout  string `mapstructure:"run_timeout"`
	StartProducers      bool   `mapstructure:"start_producers"`
	StartBroker         bool   `mapstructure:"start_broker"`
	StartConsumers      bool   `mapstructure:"start_consumers"`
	Seed                string `mapstructure:"seed"`
}

func (c *Config) RunTimeout() (time.Duration, error) {
	return time.ParseDuration(c.InternalRunTimeout)
}

const (
	subsystem         = "common"
	subsystemBroker   = "broker"
	subsystemProducer = "producer"
	subsystemConsumer = "consumer"
)

func main() {
	go exporter.SetupStandardMetricsExporter(subsystem) // TODO use fine grained subsystems after switching to dedicated service starting code (broker, producer, consumer)
	run()
}

func run() {
	logger := logrus.New()

	// load configuration
	cfg, err := loadConfig(logger)
	if err != nil {
		panic(err)
	}
	timeout, err := cfg.RunTimeout()
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)

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
		total := cfg.TopicCount * cfg.MessagesPerTopic
		duration := time.Now().Sub(start)
		throughput := math.Round(float64(total) / duration.Seconds())
		logger.Infof("finished in %s throughput=%.0f events/s", duration.String(), throughput)
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

func loadConfig(logger *logrus.Logger) (Config, error) {
	v := viper.New()
	v.SetConfigType("yaml") // REQUIRED if the config file does not have the extension in the name
	v.AddConfigPath(".")
	v.SetConfigName("config")
	v.AutomaticEnv()

	var cfg Config
	err := v.ReadInConfig()
	if err != nil {
		return cfg, err
	}

	err = v.Unmarshal(&cfg)
	overrideConfigByCLIParams(&cfg, logger)

	return cfg, err
}

func overrideConfigByCLIParams(cfg *Config, logger *logrus.Logger) {
	s := reflect.ValueOf(cfg).Elem()
	for i := 0; i < s.NumField(); i++ {
		v := s.Field(i)
		tagName, ok := s.Type().Field(i).Tag.Lookup("mapstructure")
		if !ok && !s.Field(i).CanSet() {
			continue
		}
		overrideField(tagName, v, logger)
	}
}

func overrideField(tagName string, v reflect.Value, logger *logrus.Logger) {
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
		case reflect.Bool:
			boolValue, err := strconv.ParseBool(val)
			if err != nil {
				logger.WithError(err).Warnf("Could not parse boolean value for param: %s", tagName)
				break
			}
			v.SetBool(boolValue)
		}
	}
}

func startBroker(ctx context.Context, cfg Config, logger *logrus.Logger, stopEverything context.CancelFunc) error {
	catalog := overseer.NewCatalog()
	var storageClient storageTimeline.Repository
	switch cfg.StorageType {
	case "memory":
		storageClient = inmemory.New(catalog)
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
	greeter := carrier.NewGreeter()
	lis, err := net.Listen("tcp", cfg.BrokerHost)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	ser := grpc.NewServer(
		grpc.CustomCodec(flatbuffers.FlatbuffersCodec{}),
		grpc.ConnectionTimeout(time.Second*120),
		grpc.MaxConcurrentStreams(uint32(cfg.TopicCount*(cfg.ConsumersPerTopic+cfg.ProducersPerTopic))))
	grpServer := carrier.NewGRPCServer(nil)
	coordinatorConfig := carrier.Config{}
	dealer := carrier.NewExclusiveDealer()
	supervisor := carrier.NewCoordinator(ctx, &coordinatorConfig, storageClient, catalog, greeter, dealer)
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
	//wgProducers := sync.WaitGroup{}
	leftToProduce := config.msgsCount
	aproxCountPerGroup := leftToProduce / config.producerGroupsCount

	for pi := 0; pi < config.producerGroupsCount; pi++ {
		thisGroupShare := aproxCountPerGroup
		//if is the last one, get the rest of the messages
		if pi == config.producerGroupsCount-1 {
			thisGroupShare = leftToProduce
		}
		leftToProduce -= thisGroupShare

		go func(producerGroupID string, toProduce int) {
			//TODO add more producers per group
			pc := sync_produce.SyncProduceConfig{
				Strategy:               sync_produce.StrategySingleBurst,
				SingleBurstEventsCount: toProduce,
				BatchSize:              config.batchSize,
				EventTimelineMinDelta:  config.produceDeltaMin,
				EventTimelineMaxDelta:  config.produceDeltaMax,
				BodySizeBytes:          12 * 1024,
				DeterministicEventID:   true,
			}

			p := client.NewProducer(&producer.Config{
				Cluster:         "",
				Topic:           config.topic,
				ProducerGroupID: producerGroupID,
				ProducerID:      fmt.Sprintf("%s:%d", producerGroupID, rand.Int()),
			})
			err := p.Handshake(ctx)
			if err != nil {
				logger.Error(err)
			}

			err = sync_produce.Produce(ctx, &pc, p, logger)
			if err != nil {
				logger.Error(err)
			}
		}(fmt.Sprintf("producer_group_%d", pi), thisGroupShare)
	}
	//wgProducers.Wait()
	//logger.Infof("Successfully produced  %d messages on topic=%s", config.msgsCount, config.topic)
}

func runConsumers(ctx context.Context, client brokerClient.Client, logger logrus.FieldLogger, config *deployConfig) {
	msgCounter := new(atomic.Int64)
	msgCounter.Add(int64(config.msgsCount))
	consumersCtx, closeConsumers := context.WithCancel(ctx)

	for ci := 0; ci < config.consumersCount; ci++ {
		go func(consumerID string, counter *atomic.Int64) {
			logger = logger.WithField("sync_consumer", consumerID)
			cc := sync_consume.SyncConsumeConfig{
				Consumer: client.NewConsumer(&consumer.Config{
					ConsumerID:             consumerID,
					Topic:                  config.topic,
					Cluster:                "",
					MaxBufferSize:          config.consumersBufferSize,
					LeaseDuration:          time.Minute, // TODO extract to config
					UpdatePreloadStatsTick: time.Second,
				}),
			}
			avgFullRoundTripNs, err := sync_consume.Consume(consumersCtx, counter, &cc, logger)
			if err != nil {
				logger.WithError(err).Error("sync consuming failed")
			}
			logger.Infof("avg round trip was %s", time.Duration(avgFullRoundTripNs).String())
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
			closeConsumers()
			logger.Errorf("Failed to consume all the produced messages, %d left on topic=%s", msgCounter.Load(), config.topic)
			time.Sleep(time.Second) //wait for propagation
			return
		}
	}
}
