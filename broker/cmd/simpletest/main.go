package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/prometheus/common/log"

	"github.com/bgadrian/dejaq-broker/client/timeline/producer"

	"github.com/bgadrian/dejaq-broker/client/timeline/consumer"
	"github.com/bgadrian/dejaq-broker/common/metrics/exporter"

	"github.com/bgadrian/dejaq-broker/broker/pkg/synchronization/etcd"
	"go.etcd.io/etcd/clientv3"

	"github.com/bgadrian/dejaq-broker/common"

	"github.com/sirupsen/logrus"

	"github.com/bgadrian/dejaq-broker/common/timeline"

	"github.com/bgadrian/dejaq-broker/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
	"google.golang.org/grpc"

	"github.com/bgadrian/dejaq-broker/broker/pkg/coordinator"
	"github.com/bgadrian/dejaq-broker/broker/pkg/storage/inmemory"
)

const (
	subsystem = "broker"
)

func main() {
	go exporter.SetupStandardMetricsExporter(subsystem)

	ctx, cancel := context.WithCancel(context.Background())
	logger := logrus.New()
	greeter := coordinator.NewGreeter()

	lis, err := net.Listen("tcp", "0.0.0.0:9000")
	if err != nil {
		logger.Fatalf("Failed to listen: %v", err)
	}
	ser := grpc.NewServer(grpc.CustomCodec(flatbuffers.FlatbuffersCodec{}))

	grpServer := coordinator.NewGRPCServer(nil, greeter)

	mem := inmemory.NewInMemory()

	etcdCLI, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379", "localhost:22379", "localhost:32379"},
		DialTimeout: 5 * time.Second,
	})
	if err == nil {
		defer etcdCLI.Close()
	}

	synchronization := etcd.NewEtcd(etcdCLI)

	coordinatorConfig := coordinator.Config{
		NoBuckets:    1,
		TopicType:    common.TopicType_Timeline,
		TickInterval: time.Millisecond * 50,
	}

	supervisor := coordinator.NewCoordinator(ctx, &coordinatorConfig, mem, synchronization, greeter)
	supervisor.AttachToServer(grpServer)

	go func() {
		//wait for the server to be up
		time.Sleep(time.Second)

		//CLIENT
		conn, err := grpc.Dial("127.0.0.1:9000", grpc.WithInsecure(), grpc.WithCodec(flatbuffers.FlatbuffersCodec{}))
		if err != nil {
			logger.Fatalf("Failed to connect: %v", err)
		}
		defer conn.Close()
		defer logger.Println("closing CLIENT goroutine")

		consumer.NewConsumer(ctx, conn, conn, &consumer.Config{
			ConsumerID: "alfa",
			Topic:      "topicOne",
			Cluster:    "",
			LeaseMs:    30 * 1000,
			ProcessMessageListener: func(lease timeline.PushLeases) {
				//Process the messages
				logger.Printf("received message ID=%s body=%s\n", lease.Message.ID, string(lease.Message.Body))
			},
		})
		log.Info("consumer handshake success")

		creator := producer.NewProducer(conn, conn, &producer.Config{
			Cluster:         "",
			Topic:           "topicOne",
			ProducerGroupID: "ProducerMega",
		})
		if err := creator.Handshake(ctx); err != nil {
			log.Fatal("producer handshake failed", err)
		}
		log.Info("producer handshake success")

		count := 0
		for {
			select {
			case <-time.After(time.Millisecond * 1000):
				msgCountBatch := 3
				logger.Printf("inserting %d messages\n", msgCountBatch)
				var batch []timeline.Message
				for i := 0; i < msgCountBatch; i++ {
					batch = append(batch, timeline.Message{
						ID:   []byte(fmt.Sprintf("ID %d", count+i)),
						Body: []byte(fmt.Sprintf("BODY %d", count+i)),
					})
				}
				err := creator.InsertMessages(ctx, batch)
				if err != nil {
					logger.Printf("InsertMessages ERROR %s", err.Error())
				}

				count += msgCountBatch
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		defer logger.Println("closing SERVER goroutine")

		DejaQ.RegisterBrokerServer(ser, grpServer)
		log.Info("start server")
		if err := ser.Serve(lis); err != nil {
			logger.Printf("Failed to serve: %v", err)
		}
	}()
	go func() {
		for range ctx.Done() {
		}
		logger.Println("sending server stop")
		ser.GracefulStop()
	}()

	//Wait for CTRL+C
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	// Block until we receive our signal.
	<-c
	logger.Println("shutting down signal received, waiting ...")
	cancel() //propagate trough the context
	os.Exit(0)
}
