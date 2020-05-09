package main

import (
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/dejaq/prototype/brokercete/server"

	"github.com/dejaq/prototype/grpc/DejaQ"
	"github.com/dgraph-io/badger/v2"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/ilyakaznacheev/cleanenv"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type Config struct {
	//default listens on all interfaces, this is standard for containers
	BrokerBindingAddress string `env:"DEJAQ_ADDRESS" env-default:"127.0.0.1:9000"`
	NodeID               string `env:"NODE_ID" env-default:"cete1"`
	//RaftBindingAddress string `env:"RAFT_ADDRESS" env-default:"127.0.0.1:8100"`
	DataDirectory string `env:"DATA_DIRECTORY" env-default:"/tmp/dejaq-data-node1"`
}

func main() {

	logger := logrus.New().WithField("component", "brokermain")

	c := &Config{}
	err := cleanenv.ReadEnv(c)
	if err != nil {
		logger.Fatal(err)
	}

	db, err := badger.Open(badger.DefaultOptions(c.DataDirectory))
	if err != nil {
		logger.Fatal(err)
	}

	//TODO add RAFT

	metadata := server.NewMetadata(db, logger, 3)

	//Dejaq stuff
	ser := grpc.NewServer(
		grpc.CustomCodec(flatbuffers.FlatbuffersCodec{}),
	)
	DejaQ.RegisterBrokerServer(ser, server.NewGRPC(db, logger, metadata))
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

	quitCh := make(chan os.Signal, 1)
	signal.Notify(quitCh, os.Kill, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	// wait for receiving signal
	<-quitCh

	ser.Stop()
	db.Close()
}
