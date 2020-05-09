package main

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
	"unsafe"

	"google.golang.org/grpc"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/dejaq/prototype/grpc/DejaQ"

	"github.com/mosuka/cete/log"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/mosuka/cete/client"
	"github.com/mosuka/cete/protobuf"
	"github.com/mosuka/cete/server"
	"github.com/sirupsen/logrus"
)

type Config struct {
	//default listens on all interfaces, this is standard for containers
	BrokerBindingAddress   string `env:"DEJAQ_ADDRESS" env-default:"127.0.0.1:9000"`
	CeteNodeID             string `env:"CETE_ID" env-default:"cete1"`
	CeteRaftBindingAddress string `env:"CETE_RAFT_ADDRESS" env-default:"127.0.0.1:8100"`
	CeteGRPCBindingAddress string `env:"CETE_GRPC_ADDRESS" env-default:"127.0.0.1:8101"`
	CeteHTTPBindingAddress string `env:"CETE_HTTP_ADDRESS" env-default:"127.0.0.1:8102"`
	CetePeerGRPC           string `env:"CETE_PEER_GRPC_ADDRESS" env-default:"127.0.0.1:8101"`
	CeteDataDirectory      string `env:"CETE_DATA_DIRECTORY" env-default:"/tmp/dejaq-data-node1"`
}

func main() {

	logger := logrus.New().WithField("component", "brokermain")

	c := &Config{}
	err := cleanenv.ReadEnv(c)
	if err != nil {
		logger.Fatal(err)
	}

	certificateFile := ""
	keyFile := ""

	ceteLogger := log.NewLogger(
		"DEBUG",
		"",
		1024,
		1,
		1,
		false,
	)

	bootstrap := c.CeteGRPCBindingAddress == "" || c.CetePeerGRPC == c.CeteGRPCBindingAddress

	raftServer, err := server.NewRaftServer(c.CeteNodeID, c.CeteRaftBindingAddress, c.CeteDataDirectory, bootstrap, ceteLogger)
	if err != nil {
		logger.WithError(err).Fatal("cannot init raft")
	}

	grpcServer, err := server.NewGRPCServer(c.CeteGRPCBindingAddress, raftServer, certificateFile, keyFile, c.CeteNodeID, ceteLogger)
	if err != nil {
		logger.WithError(err).Fatal("cannot init cete GRPC")
	}

	grpcGateway, err := server.NewGRPCGateway(c.CeteHTTPBindingAddress, c.CeteGRPCBindingAddress, certificateFile, keyFile, c.CeteNodeID, ceteLogger)
	if err != nil {
		logger.WithError(err).Fatal("cannot init cete GRPC gateway")
	}

	quitCh := make(chan os.Signal, 1)
	signal.Notify(quitCh, os.Kill, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	if err := raftServer.Start(); err != nil {
		logger.WithError(err).Fatal("cannot start cete Raft")
	}

	if err := grpcServer.Start(); err != nil {
		logger.WithError(err).Fatal("cannot start cete GRPC")
	}

	if err := grpcGateway.Start(); err != nil {
		logger.WithError(err).Fatal("cannot start cete GRPC gateway")
	}

	// wait for detect leader if it's bootstrap
	if bootstrap {
		timeout := 60 * time.Second
		if err := raftServer.WaitForDetectLeader(timeout); err != nil {
			logger.WithError(err).Fatal("timeout WaitForDetectLeader  ")
		}
	}

	// create gRPC client for joining node
	var joinGrpcAddress string
	if bootstrap {
		joinGrpcAddress = c.CeteGRPCBindingAddress
	} else {
		joinGrpcAddress = c.CetePeerGRPC
	}

	ceteClient, err := client.NewGRPCClientWithContextTLS(joinGrpcAddress, context.Background(), certificateFile, c.CeteNodeID)
	if err != nil {
		logger.WithError(err).Fatal("timeout NewGRPCClientWithContextTLS  ")
	}
	defer func() {
		_ = ceteClient.Close()
	}()

	// join this node to the existing cluster
	joinRequest := &protobuf.JoinRequest{
		Id: c.CeteNodeID,
		Node: &protobuf.Node{
			RaftAddress: c.CeteRaftBindingAddress,
			Metadata: &protobuf.Metadata{
				GrpcAddress: c.CeteGRPCBindingAddress,
				HttpAddress: c.CeteHTTPBindingAddress,
			},
		},
	}
	if err = ceteClient.Join(joinRequest); err != nil {
		logger.WithError(err).Fatal("cete client join failed")
	}

	//Dejaq stuff
	ser := grpc.NewServer(
		grpc.CustomCodec(flatbuffers.FlatbuffersCodec{}),
	)
	DejaQ.RegisterBrokerServer(ser, DejaqGrpc{
		raft:   raftServer,
		logger: logger,
	})
	lis, err := net.Listen("tcp", c.BrokerBindingAddress)
	if err != nil {
		logger.Fatalf("failed to listen: %w", err)
	}
	go func() {
		logger.Infof("start dejaq grpc server %s", c.BrokerBindingAddress)
		if err := ser.Serve(lis); err != nil {
			logger.WithError(err).Error("grpc server failed")
		}
	}()

	// wait for receiving signal
	<-quitCh

	_ = grpcGateway.Stop()
	_ = grpcServer.Stop()
	_ = raftServer.Stop()
}

type DejaqGrpc struct {
	raft   *server.RaftServer
	logger logrus.FieldLogger
}

func (d DejaqGrpc) Produce(stream DejaQ.Broker_ProduceServer) error {
	var err error
	var request *DejaQ.ProduceRequest
	//gather all the messages from the client
	for err == nil {
		request, err = stream.Recv()
		if request == nil { //empty msg ?!?!?! TODO log this as a warning
			continue
		}
		if err == io.EOF { //it means the stream batch is over
			break
		}
		if err != nil {
			_ = fmt.Errorf("grpc server TimelineCreateMessages client failed err=%s", err.Error())
			break
		}

		werr := d.raft.Set(&protobuf.SetRequest{
			Key:   generateMsgKey(1),
			Value: request.BodyBytes(),
		})
		if werr != nil {
			err = werr
			break
		}
	}

	//returns the response to the client
	var builder *flatbuffers.Builder
	builder = flatbuffers.NewBuilder(128)

	DejaQ.ProduceResponseStart(builder)
	if err == nil {
		DejaQ.ProduceResponseAddAck(builder, true)
	} else {
		d.logger.WithError(err).Error("failed to produce msg")
		DejaQ.ProduceResponseAddAck(builder, false)
	}
	root := DejaQ.ProduceResponseEnd(builder)
	builder.Finish(root)
	err = stream.SendMsg(builder)
	if err != nil {
		d.logger.WithError(err).Error("Produce failed")
	}

	return nil
}

func (d DejaqGrpc) Consume(DejaQ.Broker_ConsumeServer) error {
	panic("implement me")
}

func generateMsgKey(partitionID uint16) string {
	randomMsgId := make([]byte, 6)
	rand.Read(randomMsgId)

	buf := make([]byte, 2, 8)
	// little-endian uint16 into a byte slice.
	_ = buf[7] // Force one bounds check. See: golang.org/issue/14808
	buf[0] = byte(partitionID)
	buf[1] = byte(partitionID >> 8)

	buf = append(buf, randomMsgId...)
	return *(*string)(unsafe.Pointer(&buf))
}
