package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/bgadrian/dejaq-broker/broker/pkg/coordinator"
	"github.com/bgadrian/dejaq-broker/broker/pkg/overseer"
	"github.com/bgadrian/dejaq-broker/broker/pkg/storage/cockroach"
	"github.com/bgadrian/dejaq-broker/broker/pkg/storage/redis"
	storageTimeline "github.com/bgadrian/dejaq-broker/broker/pkg/storage/timeline"
	brokerClient "github.com/bgadrian/dejaq-broker/client"
	"github.com/bgadrian/dejaq-broker/client/satellite"
	"github.com/bgadrian/dejaq-broker/client/timeline/consumer"
	"github.com/bgadrian/dejaq-broker/client/timeline/producer"
	"github.com/bgadrian/dejaq-broker/client/timeline/sync_consume"
	"github.com/bgadrian/dejaq-broker/client/timeline/sync_produce"
	"github.com/bgadrian/dejaq-broker/common/timeline"
	"github.com/bgadrian/dejaq-broker/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/prometheus/common/log"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"go.uber.org/atomic"
	"google.golang.org/grpc"

	_ "github.com/lib/pq"
)

var consumersBufferSize = int64(100)

func main() {
	topicCount := 12
	batchSize := 15
	bucketCount := uint16(52)
	msgsCountPerTopic := 33
	producersPerTopic := 2
	consumersPerTopic := 13
	runTimeout := time.Duration(70)
	//compared to now(), random TS of the produced messages
	produceDeltaMin := time.Second
	produceDeltaMax := time.Second
	overseerSeeds := "127.0.0.1:9000"
	brokerListenAddr := "127.0.0.1:9000"

	viper.BindEnv("STORAGE")
	viper.SetDefault("STORAGE", "miniredis")
	logger := logrus.New()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*runTimeout))
	err := startBroker(ctx, logger, runTimeout, brokerListenAddr)
	if err != nil {
		logger.WithError(err).Fatal("failed startBroker")
	}

	wg := sync.WaitGroup{}
	start := time.Now()
	defer func() {
		logger.Infof("finished in %dms", time.Now().Sub(start).Milliseconds())
	}()

	clientConfig := satellite.Config{
		Cluster:        "",
		OverseersSeeds: strings.Split(overseerSeeds, ","),
	}

	for topicIndex := 0; topicIndex < topicCount; topicIndex++ {
		//each topic has its own gRPC client, producers and consumers
		client, err := satellite.NewClient(ctx, logger, &clientConfig)
		if err != nil {
			logger.WithError(err).Fatal("brokerClient")
		}
		chief := client.NewOverseerClient()
		defer func() {
			client.Close()
			//logger.Println("closing gRPC CLIENT goroutine")
		}()

		topicID := fmt.Sprintf("topic_%d", topicIndex)
		wg.Add(1)
		go func(topic string) {
			defer wg.Done()

			err := chief.CreateTimelineTopic(ctx, topic, timeline.TopicSettings{
				ReplicaCount:            0,
				MaxSecondsFutureAllowed: 10,
				MaxSecondsLease:         10,
				ChecksumBodies:          false,
				MaxBodySizeBytes:        100000,
				RQSLimitPerClient:       100000,
				MinimumProtocolVersion:  0,
				MinimumDriverVersion:    0,
				BucketCount:             bucketCount,
			})
			if err != nil {
				logger.WithError(err).Fatal("failed creating topic")
				return
			}

			produceAndConsumeSync(ctx, client, logger, &deployConfig{
				producerGroupsCount: producersPerTopic,
				consumersCount:      consumersPerTopic,
				topic:               topic,
				msgsCount:           msgsCountPerTopic,
				batchSize:           batchSize,
				produceDeltaMin:     produceDeltaMin,
				produceDeltaMax:     produceDeltaMax,
			})
		}(topicID)
	}

	wg.Wait()
	logger.Info("all topics finished, closing the clients")
	cancel() //propagate trough the context
}

func startBroker(ctx context.Context, logger *logrus.Logger, timeoutSeconds time.Duration, brokerListenAddr string) error {
	var storageClient storageTimeline.Repository
	switch viper.GetString("STORAGE") {
	case "redis":
		return errors.New("storage not implemented")
	case "miniredis":
		// start in memory redis server
		redisServer, err := redis.NewServer()
		if err != nil {
			return fmt.Errorf("failed to start redis service: %w", err)
		}
		go func() {
			select {
			case <-ctx.Done():
				redisServer.Close()
			}
		}()
		// redis client that implement timeline interface
		storageClient, err = redis.New(redisServer.Addr())
		if err != nil {
			return fmt.Errorf("failed to connect to redis server: %w", err)
		}
	case "cockroach":
		// Connect to the "bank" database.
		db, err := sql.Open("postgres", "postgresql://duser@localhost:26257/dejaq?sslmode=disable&binary_parameters")
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
	lis, err := net.Listen("tcp", brokerListenAddr)
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
	produceDeltaMin, produceDeltaMax    time.Duration
}

func produceAndConsumeSync(ctx context.Context, client brokerClient.Client, logger logrus.FieldLogger, config *deployConfig) {
	wgProducers := sync.WaitGroup{}
	msgCounter := new(atomic.Int32)
	consumersCtx, closeConsumers := context.WithCancel(ctx)

	for pi := 0; pi < config.producerGroupsCount; pi++ {
		wgProducers.Add(1)
		go func(producerGroupID string, msgCounter *atomic.Int32) {
			defer wgProducers.Done()
			//TODO add more producers per group
			pc := sync_produce.SyncProduceConfig{
				Count:           config.msgsCount / config.producerGroupsCount,
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

			err := sync_produce.Produce(ctx, msgCounter, &pc)
			if err != nil {
				log.Error(err)
			}
		}(fmt.Sprintf("producer_group_%d", pi), msgCounter)
	}

	for ci := 0; ci < config.consumersCount; ci++ {
		go func(consumerID string, counter *atomic.Int32) {
			cc := sync_consume.SyncConsumeConfig{
				Consumer: client.NewConsumer(&consumer.Config{
					ConsumerID:    consumerID,
					Topic:         config.topic,
					Cluster:       "",
					MaxBufferSize: consumersBufferSize,
					LeaseDuration: time.Millisecond * 1000,
				}),
			}
			err := sync_consume.Consume(consumersCtx, counter, &cc)
			if err != nil {
				log.Error(err)
			}
		}(fmt.Sprintf("consumer_%d", ci), msgCounter)
	}

	wgProducers.Wait()
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
			logger.Panicf("Failed to consume all the produced messages, %d left on topic=%s", msgCounter.Load(), config.topic)
			return
		}
	}
}
