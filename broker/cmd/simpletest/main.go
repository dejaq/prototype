package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/bgadrian/dejaq-broker/common"

	"github.com/sirupsen/logrus"

	"github.com/bgadrian/dejaq-broker/common/timeline"

	"github.com/bgadrian/dejaq-broker/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
	"google.golang.org/grpc"

	"github.com/bgadrian/dejaq-broker/broker/pkg/coordinator"
	"github.com/bgadrian/dejaq-broker/broker/pkg/storage/inmemory"
)

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	logger := logrus.New()

	lis, err := net.Listen("tcp", "0.0.0.0:9000")
	if err != nil {
		logger.Fatalf("Failed to listen: %v", err)
	}
	ser := grpc.NewServer(grpc.CustomCodec(flatbuffers.FlatbuffersCodec{}))

	grpServer := coordinator.NewGRPCServer(nil)

	mem := inmemory.NewInMemory()

	coordinatorConfig := coordinator.CoordinatorConfig{
		NoBuckets:    5,
		TopicType:    common.TopicType_Timeline,
		TickInterval: time.Millisecond * 50,
	}

	coordinator.NewCoordinator(ctx, coordinatorConfig, mem, grpServer)

	go func() {
		//CLIENT
		conn, err := grpc.Dial("0.0.0.0:9000", grpc.WithInsecure(), grpc.WithCodec(flatbuffers.FlatbuffersCodec{}))
		if err != nil {
			logger.Fatalf("Failed to connect: %v", err)
		}
		defer conn.Close()
		defer logger.Println("closing CLIENT goroutine")

		grpcClient := coordinator.NewGRPCClient(conn, coordinator.ClientConsumer{
			ConsumerID: nil,
			Topic:      "default_timeline",
			Cluster:    "",
			LeaseMs:    30 * 1000,
		})
		grpcClient.ProcessMessageListener = func(lease timeline.PushLeases) {
			//Process the messages
			logger.Printf("received message ID=%s body=%s\n", lease.Message.ID, string(lease.Message.Body))
		}

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
				err := grpcClient.InsertMessages(ctx, batch)
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

		DejaQ.RegisterBrokerServer(ser, grpServer.InnerServer)
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
