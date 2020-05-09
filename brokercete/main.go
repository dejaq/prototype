package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mosuka/cete/log"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/mosuka/cete/client"
	"github.com/mosuka/cete/protobuf"
	"github.com/mosuka/cete/server"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type Config struct {
	//default listens on all interfaces, this is standard for containers
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

	certificateFile := viper.GetString("certificate_file")
	keyFile := viper.GetString("key_file")

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

	// wait for receiving signal
	<-quitCh

	_ = grpcGateway.Stop()
	_ = grpcServer.Stop()
	_ = raftServer.Stop()
}
