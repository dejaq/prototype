package main

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/bgadrian/dejaq-broker/client/satellite"

	"github.com/bgadrian/dejaq-broker/common/timeline"

	"github.com/bgadrian/dejaq-broker/client/timeline/consumer"
	"github.com/bgadrian/dejaq-broker/client/timeline/sync_consume"

	"github.com/bgadrian/dejaq-broker/client/timeline/sync_produce"

	"github.com/bgadrian/dejaq-broker/broker/pkg/overseer"

	"go.uber.org/atomic"

	"github.com/bgadrian/dejaq-broker/broker/pkg/coordinator"
	"github.com/bgadrian/dejaq-broker/broker/pkg/storage/redis"
	brokerClient "github.com/bgadrian/dejaq-broker/client"
	"github.com/bgadrian/dejaq-broker/client/timeline/producer"
	"github.com/bgadrian/dejaq-broker/common"
	"github.com/bgadrian/dejaq-broker/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/prometheus/common/log"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func main() {
	msgsCount := 123
	batchSize := 15
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*15))

	logger := logrus.New()
	greeter := coordinator.NewGreeter()

	lis, err := net.Listen("tcp", "127.0.0.1:9000")
	if err != nil {
		logger.Fatalf("Failed to listen: %v", err)
	}
	ser := grpc.NewServer(grpc.CustomCodec(flatbuffers.FlatbuffersCodec{}))

	grpServer := coordinator.NewGRPCServer(nil)

	// start in memory redis server
	redisServer, err := redis.NewServer()
	if err != nil {
		logger.Fatalf("Failed to start redis service: %v", err)
	}

	// redis client that implement timeline interface
	redisClient, err := redis.New(redisServer.Addr())
	if err != nil {
		logger.Fatalf("Failed to connect to redis server: %v", err)
	}

	coordinatorConfig := coordinator.Config{
		NoBuckets: 89,
		TopicType: common.TopicType_Timeline,
	}

	dealer := coordinator.NewExclusiveDealer()
	synchronization := overseer.NewCatalog()

	supervisor := coordinator.NewCoordinator(ctx, &coordinatorConfig, redisClient, synchronization, greeter,
		coordinator.NewLoader(&coordinator.LConfig{TopicDefaultNoOfBuckets: coordinatorConfig.NoBuckets},
			redisClient, dealer, greeter), dealer)
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
		Cluster: "",
		Seeds:   []string{"127.0.0.1:9000"},
	}
	client, err := satellite.NewClient(ctx, logrus.New(), &clientConfig)
	if err != nil {
		logrus.WithError(err).Fatal("brokerClient")
	}
	defer client.Close()
	defer logger.Println("closing CLIENT goroutine")
	overseer := client.NewOverseerClient()

	topics := []string{"topic_1", "topic_2", "topic_3"}

	producerGroupIDs := []string{"producer_group_1", "producer_group_1", "producer_group_2"}
	consumerIDs := []string{"consumer_id_1", "consumer_id_2", "consumer_id_3", "consumer_id_4"}

	wg := sync.WaitGroup{}

	for _, topic := range topics {
		wg.Add(1)
		go func(topic string) {
			defer wg.Done()
			err := overseer.CreateTimelineTopic(ctx, topic, timeline.TopicSettings{
				ReplicaCount:            0,
				MaxSecondsFutureAllowed: 10,
				MaxSecondsLease:         10,
				ChecksumBodies:          false,
				MaxBodySizeBytes:        100000,
				RQSLimitPerClient:       100000,
				MinimumProtocolVersion:  0,
				MinimumDriverVersion:    0,
				BucketCount:             100,
			})
			if err != nil {
				logrus.WithError(err).Fatal("failed creating topic")
			}
			deployTopicTest(ctx, client, producerGroupIDs, consumerIDs, topic, msgsCount, batchSize)
		}(topic)
	}

	wg.Wait()

	cancel() //propagate trough the context
	ser.GracefulStop()
}

func deployTopicTest(ctx context.Context, client brokerClient.Client, producerGroupIDs, consumerIDs []string, topic string, msgsCount, batchSize int) {

	wg := sync.WaitGroup{}
	msgCounter := new(atomic.Int32)

	for _, producerGroupID := range producerGroupIDs {
		wg.Add(1)
		go func(producerGroupID string, msgCounter *atomic.Int32) {
			defer wg.Done()
			pc := sync_produce.SyncProduceConfig{
				Count:     msgsCount / len(producerGroupIDs),
				BatchSize: batchSize,
				Producer: client.NewProducer(&producer.Config{
					Cluster:         "",
					Topic:           topic,
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

	time.Sleep(time.Second) // sleep to allow producers to handshake and create topic

	for _, consumerID := range consumerIDs {
		wg.Add(1)
		go func(consumerID string, counter *atomic.Int32) {
			defer wg.Done()
			cc := sync_consume.SyncConsumeConfig{
				Consumer: client.NewConsumer(&consumer.Config{
					ConsumerID:    consumerID,
					Topic:         topic,
					Cluster:       "",
					MaxBufferSize: 100,
					LeaseDuration: time.Millisecond * 1000,
				}),
			}
			err := sync_consume.Consume(ctx, counter, &cc)
			if err != nil {
				log.Error(err)
			}
		}(consumerID, msgCounter)
	}

	wg.Wait() //wait for consumers and producers

	if msgCounter.Load() == 0 {
		logrus.Infof("Successfully produced and consumed %d messages on topic=%s", msgsCount, topic)
	} else {
		logrus.Errorf("Failed to consume all the produced messages, %d left on topic=%s", msgCounter.Load(), topic)
	}
}
