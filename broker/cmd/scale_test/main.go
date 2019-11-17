package main

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/bgadrian/dejaq-broker/broker/pkg/coordinator"
	"github.com/bgadrian/dejaq-broker/broker/pkg/storage/redis"
	"github.com/bgadrian/dejaq-broker/client/timeline/consumer"
	"github.com/bgadrian/dejaq-broker/client/timeline/producer"
	"github.com/bgadrian/dejaq-broker/common"
	dtime "github.com/bgadrian/dejaq-broker/common/time"
	"github.com/bgadrian/dejaq-broker/common/timeline"
	"github.com/bgadrian/dejaq-broker/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/pkg/errors"
	"github.com/prometheus/common/log"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func main() {
	msgsCount := 50
	batchSize := 15
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*15))

	logger := logrus.New()
	greeter := coordinator.NewGreeter()

	lis, err := net.Listen("tcp", "127.0.0.1:9000")
	if err != nil {
		logger.Fatalf("Failed to listen: %v", err)
	}
	ser := grpc.NewServer(grpc.CustomCodec(flatbuffers.FlatbuffersCodec{}))

	grpServer := coordinator.NewGRPCServer(nil, greeter)

	// start in memory redis server
	redisServer, err := redis.NewServer()
	if err != nil {
		logger.Fatalf("Failed to start redis service: %v", err)
	}

	// redis client that implement timeline interface
	redisClient, err := redis.NewClient(redisServer.Addr())
	if err != nil {
		logger.Fatalf("Failed to connect to redis server: %v", err)
	}

	coordinatorConfig := coordinator.Config{
		NoBuckets: 89,
		TopicType: common.TopicType_Timeline,
	}

	dealer := coordinator.NewExclusiveDealer()

	supervisor := coordinator.NewCoordinator(ctx, &coordinatorConfig, redisClient, nil, greeter,
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

	//CLIENT
	conn, err := grpc.Dial("127.0.0.1:9000", grpc.WithInsecure(), grpc.WithCodec(flatbuffers.FlatbuffersCodec{}))
	if err != nil {
		logger.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()
	defer logger.Println("closing CLIENT goroutine")

	topics := []string{"topic_1", "topic_2", "topic_3"}

	producerGroupIDs := []string{"producer_group_1", "producer_group_1", "producer_group_2"}
	consumerIDs := []string{"consumer_id_1", "consumer_id_2", "consumer_id_3", "consumer_id_4"}

	wg := sync.WaitGroup{}

	for _, topic := range topics {
		wg.Add(1)
		go func(topic string) {
			defer wg.Done()
			deployTopicTest(ctx, conn, producerGroupIDs, consumerIDs, topic, msgsCount, batchSize)
		}(topic)
	}

	wg.Wait()

	cancel() //propagate trough the context
	ser.GracefulStop()
}

func deployTopicTest(ctx context.Context, conn *grpc.ClientConn, producerGroupIDs, consumerIDs []string, topic string, msgsCount, batchSize int) {

	wg := sync.WaitGroup{}
	msgCounter := new(atomic.Int32)
	for _, producerGroupID := range producerGroupIDs {
		wg.Add(1)
		go func(producerGroupID string, msgCounter *atomic.Int32) {
			defer wg.Done()
			err := Produce(ctx, conn, msgCounter, &PConfig{
				Count:           msgsCount / len(producerGroupIDs),
				Cluster:         "",
				Topic:           topic,
				ProducerGroupID: producerGroupID,
				BatchSize:       batchSize,
			})
			if err != nil {
				log.Error(err)
			}
		}(producerGroupID, msgCounter)
	}

	for _, consumerID := range consumerIDs {
		wg.Add(1)
		go func(consumerID string, counter *atomic.Int32) {
			defer wg.Done()
			err := Consume(ctx, conn, counter, &CConfig{
				WaitForCount:  msgsCount,
				Cluster:       "",
				Topic:         topic,
				ConsumerID:    consumerID,
				MaxBufferSize: 100,
				Lease:         time.Millisecond * 1000,
			})
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

type PConfig struct {
	Count           int
	Cluster         string
	Topic           string
	ProducerGroupID string
	BatchSize       int
}

func Produce(ctx context.Context, conn *grpc.ClientConn, msgCounter *atomic.Int32, config *PConfig) error {
	creator := producer.NewProducer(conn, conn, &producer.Config{
		Cluster:         config.Cluster,
		Topic:           config.Topic,
		ProducerGroupID: config.ProducerGroupID,
	}, config.ProducerGroupID+"_producer"+strconv.Itoa(rand.Int()))
	if err := creator.Handshake(ctx); err != nil {
		return errors.Wrap(err, "producer handshake failed")
	}
	log.Info("producer handshake success")

	t := time.Now().UTC()
	left := config.Count
	var batch []timeline.Message

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := creator.InsertMessages(ctx, batch)
		if err != nil {
			return errors.Wrap(err, "InsertMessages ERROR")
		}
		msgCounter.Add(int32(len(batch)))
		batch = batch[:0]
		return nil
	}

	for left > 0 {
		left--
		msgID := config.Count - left
		batch = append(batch, timeline.Message{
			ID:          []byte(fmt.Sprintf("ID %s|msg_%d", config.ProducerGroupID, msgID)),
			Body:        []byte(fmt.Sprintf("BODY %s|msg_%d", config.ProducerGroupID, msgID)),
			TimestampMS: dtime.TimeToMS(t.Add(time.Millisecond + time.Duration(msgID))),
		})
		if len(batch) >= config.BatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	log.Infof("inserted %d messages group=%s on topic=%s", config.Count, config.ProducerGroupID, config.Topic)
	return nil
}

type CConfig struct {
	WaitForCount  int
	Cluster       string
	Topic         string
	ConsumerID    string
	MaxBufferSize int64
	Lease         time.Duration
	Timeout       time.Duration
}

func Consume(ctx context.Context, conn *grpc.ClientConn, msgsCounter *atomic.Int32, conf *CConfig) error {
	c := consumer.NewConsumer(conn, conn, &consumer.Config{
		ConsumerID:    conf.ConsumerID,
		Topic:         conf.Topic,
		Cluster:       conf.Cluster,
		LeaseDuration: conf.Lease,
		MaxBufferSize: conf.MaxBufferSize,
	})

	c.Start(ctx, func(lease timeline.PushLeases) {
		if lease.GetConsumerID() != conf.ConsumerID {
			log.Fatalf("server sent message for another consumer me=%s sent=%s", conf.ConsumerID, lease.GetConsumerID())
		}
		//Process the messages
		msgsCounter.Dec()
		err := c.Delete(ctx, []timeline.Message{{
			ID:          lease.Message.ID,
			TimestampMS: lease.Message.TimestampMS,
			BucketID:    lease.Message.BucketID,
			Version:     lease.Message.Version,
		}})
		if err != nil {
			log.Errorf("delete failed", err)
		}
		//logrus.Printf("received message ID='%s' body='%s' from bucket=%d\n", lease.Message.ID, string(lease.Message.Body), lease.Message.BucketID)
	})

	log.Info("consumer handshake success")

	for {
		select {
		case <-ctx.Done():
			return nil
		}
	}

	return nil
}
