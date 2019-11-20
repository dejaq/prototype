package main

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

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
	msgsCount := 100
	batchSize := 100
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*5))

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
	redisClient, err := redis.New(redisServer.Addr())
	if err != nil {
		logger.Fatalf("Failed to connect to redis server: %v", err)
	}

	coordinatorConfig := coordinator.Config{
		NoBuckets: 1,
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

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		err := Produce(ctx, conn, &PConfig{
			Count:           msgsCount,
			Cluster:         "",
			Topic:           "testminibench1",
			ProducerGroupID: "prod1",
			BatchSize:       batchSize,
		})
		if err != nil {
			log.Error(err)
		}
		wg.Done()
	}()
	go func() {
		err := Consume(ctx, conn, &CConfig{
			WaitForCount:  msgsCount,
			Cluster:       "",
			Topic:         "testminibench1",
			ConsumerID:    "consumer1",
			MaxBufferSize: 100,
			Lease:         time.Millisecond * 20000,
		})
		if err != nil {
			log.Error(err)
		}
		wg.Done()
	}()

	wg.Wait() //wait for consuemrs and producers

	//go func() {
	//	for range ctx.Done() {
	//	}
	//	logger.Println("sending server stop")
	cancel() //propagate trough the context
	ser.GracefulStop()
	//}()
	//
	////Wait for CTRL+C
	//c := make(chan os.Signal, 1)
	//signal.Notify(c, os.Interrupt)
	//// Block until we receive our signal.
	//<-c
	//logger.Println("shutting down signal received, waiting ...")
	//os.Exit(0)
}

type PConfig struct {
	Count           int
	Cluster         string
	Topic           string
	ProducerGroupID string
	BatchSize       int
}

func Produce(ctx context.Context, conn *grpc.ClientConn, config *PConfig) error {
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
		batch = batch[:0]
		return nil
	}

	for left > 0 {
		left--
		msgID := config.Count - left
		batch = append(batch, timeline.Message{
			ID:          []byte(fmt.Sprintf("ID %d", msgID)),
			Body:        []byte(fmt.Sprintf("BODY %d", msgID)),
			TimestampMS: dtime.TimeToMS(t.Add(time.Millisecond * time.Duration(msgID))),
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
	log.Infof("finished inserting %d messages", config.Count)
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

func Consume(ctx context.Context, conn *grpc.ClientConn, conf *CConfig) error {
	wg := sync.WaitGroup{}
	wg.Add(conf.WaitForCount)

	c := consumer.NewConsumer(conn, conn, &consumer.Config{
		ConsumerID:    conf.ConsumerID,
		Topic:         conf.Topic,
		Cluster:       conf.Cluster,
		LeaseDuration: conf.Lease,
		MaxBufferSize: conf.MaxBufferSize,
	})

	c.Start(ctx, func(lease timeline.PushLeases) {
		//Process the messages
		err := c.Delete(ctx, []timeline.Message{{
			ID:          lease.Message.ID,
			TimestampMS: lease.Message.TimestampMS,
			BucketID:    lease.Message.BucketID,
			Version:     lease.Message.Version,
		}})
		if err != nil {
			log.Errorf("delete failed", err)
		}
		//logrus.Printf("received message ID=%s body=%s from bucket=%d\n", lease.Message.ID, string(lease.Message.Body), lease.Message.BucketID)
		wg.Done()
	})

	log.Info("consumer handshake success")

	go func() {
		for {
			select {
			case <-ctx.Done():
				//wg.Set(-conf.WaitForCount)
				return
			}
		}
	}()

	wg.Wait()
	logrus.Infof("consumer finished %d messages", conf.WaitForCount)
	return nil
}
