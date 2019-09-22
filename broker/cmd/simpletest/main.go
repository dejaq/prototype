package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/bgadrian/dejaq-broker/common/timeline"

	"github.com/bgadrian/dejaq-broker/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
	"google.golang.org/grpc"

	"github.com/bgadrian/dejaq-broker/broker/pkg/coordinator"
	"github.com/bgadrian/dejaq-broker/broker/pkg/storage/inmemory"
)

func main() {

	ctx, cancel := context.WithCancel(context.Background())

	lis, err := net.Listen("tcp", "0.0.0.0:9000")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	ser := grpc.NewServer(grpc.CustomCodec(flatbuffers.FlatbuffersCodec{}))

	grpServer := coordinator.NewGRPCServer(nil)

	mem := inmemory.NewInMemory()
	coordinator.NewCoordinator(ctx, mem, time.Second, grpServer)

	go func() {
		//CLIENT
		conn, err := grpc.Dial("0.0.0.0:9000", grpc.WithInsecure(), grpc.WithCodec(flatbuffers.FlatbuffersCodec{}))
		if err != nil {
			log.Fatalf("Failed to connect: %v", err)
		}
		defer conn.Close()
		defer fmt.Println("closing CLIENT goroutine")

		grpcClient := coordinator.NewGRPCClient(conn)

		count := 0
		for {
			select {
			case <-time.After(time.Second):
				count++
				err := grpcClient.InsertMessages(ctx, []timeline.Message{
					{
						ID:   []byte(fmt.Sprintf("ID %d", count)),
						Body: []byte(fmt.Sprintf("BODY %d", count)),
					},
				})
				if err != nil {
					log.Printf("InsertMessages ERROR %s", err.Error())
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		defer fmt.Println("closing SERVER goroutine")

		DejaQ.RegisterBrokerServer(ser, grpServer.InnerServer)
		if err := ser.Serve(lis); err != nil {
			log.Printf("Failed to serve: %v", err)
		}
	}()
	go func() {
		for range ctx.Done() {
		}
		fmt.Println("sending server stop")
		ser.GracefulStop()
	}()

	//Wait for CTRL+C
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	// Block until we receive our signal.
	<-c
	log.Println("shutting down signal received, waiting ...")
	cancel() //propagate trough the context
	os.Exit(0)
}
