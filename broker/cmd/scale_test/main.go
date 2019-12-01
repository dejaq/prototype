package main

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/bgadrian/dejaq-broker/broker/pkg/coordinator"
	"github.com/bgadrian/dejaq-broker/broker/pkg/overseer"
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
)

var consumersBufferSize = int64(100)

func main() {
	msgsCountPerTopic := 33
	batchSize := 15
	bucketCount := uint16(100)
	topicCount := 2
	timeoutSeconds := time.Duration(7)
	//compared to now(), random TS of the produced messages
	produceDeltaMin := time.Millisecond * 100
	produceDeltaMax := time.Duration(0) //time.Millisecond * 500

	viper.BindEnv("STORAGE")
	logger := logrus.New()

	var storageClient storageTimeline.Repository
	if viper.GetString("STORAGE") == "redis" {
		// start in memory redis server
		redisServer, err := redis.NewServer()
		if err != nil {
			logger.Fatalf("Failed to start redis service: %v", err)
		}

		// redis client that implement timeline interface
		storageClient, err = redis.New(redisServer.Addr())
		//redisClient, err := redis.New("127.0.0.1:6379")
		if err != nil {
			logger.Fatalf("Failed to connect to redis server: %v", err)
		}
	} else {

		// start in memory redis server
		redisServer, err := redis.NewServer()
		if err != nil {
			logger.Fatalf("Failed to start redis service: %v", err)
		}

		// redis client that implement timeline interface
		storageClient, err = redis.New(redisServer.Addr())
		if err != nil {
			logger.Fatalf("Failed to connect to redis server: %v", err)
		}

	}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*timeoutSeconds))

	greeter := coordinator.NewGreeter()

	lis, err := net.Listen("tcp", "127.0.0.1:9000")
	if err != nil {
		logger.Fatalf("Failed to listen: %v", err)
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

	//wait for the server to be up
	time.Sleep(time.Second)

	clientConfig := satellite.Config{
		Cluster:        "",
		OverseersSeeds: []string{"127.0.0.1:9000"},
	}
	client, err := satellite.NewClient(ctx, logrus.New(), &clientConfig)
	if err != nil {
		logrus.WithError(err).Fatal("brokerClient")
	}
	defer client.Close()
	defer logger.Println("closing CLIENT goroutine")
	chief := client.NewOverseerClient()

	producerGroupIDs := []string{"producer_group_1", "producer_group_1", "producer_group_2"}
	consumerIDs := []string{"consumer_id_1", "consumer_id_2", "consumer_id_3", "consumer_id_4"}

	wg := sync.WaitGroup{}
	start := time.Now()
	defer func() {
		logger.Infof("finished in %dms", time.Now().Sub(start).Milliseconds())
	}()

	for topicIndex := 0; topicIndex < topicCount; topicIndex++ {
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
				logrus.WithError(err).Fatal("failed creating topic")
				return
			}

			deployTopicTest(ctx, client, &deployConfig{
				producerGroupIDs: producerGroupIDs,
				consumerIDs:      consumerIDs,
				topic:            topic,
				msgsCount:        msgsCountPerTopic,
				batchSize:        batchSize,
				produceDeltaMin:  produceDeltaMin,
				produceDeltaMax:  produceDeltaMax,
			})
		}(topicID)
	}

	wg.Wait()

	cancel() //propagate trough the context
	ser.GracefulStop()
}

type deployConfig struct {
	producerGroupIDs, consumerIDs    []string
	topic                            string
	msgsCount, batchSize             int
	produceDeltaMin, produceDeltaMax time.Duration
}

func deployTopicTest(ctx context.Context, client brokerClient.Client, config *deployConfig) {

	wgProducers := sync.WaitGroup{}
	msgCounter := new(atomic.Int32)
	consumersCtx, closeConsumers := context.WithCancel(ctx)

	for _, producerGroupID := range config.producerGroupIDs {
		wgProducers.Add(1)
		go func(producerGroupID string, msgCounter *atomic.Int32) {
			defer wgProducers.Done()
			pc := sync_produce.SyncProduceConfig{
				Count:           config.msgsCount / len(config.producerGroupIDs),
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
		}(producerGroupID, msgCounter)
	}

	for _, consumerID := range config.consumerIDs {
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
		}(consumerID, msgCounter)
	}

	wgProducers.Wait()
	checker := time.NewTicker(time.Millisecond * 10)
	defer checker.Stop()

	for {
		select {
		case <-checker.C:
			if msgCounter.Load() == 0 {
				closeConsumers()
				logrus.Infof("Successfully produced and consumed %d messages on topic=%s", config.msgsCount, config.topic)
				return
			}
		case <-ctx.Done():
			logrus.Panicf("Failed to consume all the produced messages, %d left on topic=%s", msgCounter.Load(), config.topic)
			return
		}
	}
}
