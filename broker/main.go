package main

import (
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dejaq/prototype/broker/server"
	"github.com/dejaq/prototype/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/ilyakaznacheev/cleanenv"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type Config struct {
	//default listens on all interfaces, this is standard for containers
	BrokerBindingAddress string `env:"DEJAQ_ADDRESS" env-default:"127.0.0.1:9000"`
	NodeID               string `env:"NODE_ID" env-default:"broker1"`
	Partitions           int    `env:"PARTITIONS" env-default:"32"`
	//RaftBindingAddress string `env:"RAFT_ADDRESS" env-default:"127.0.0.1:8100"`
	DataDirectory string `env:"DATA_DIRECTORY" env-default:"/tmp/dejaq-data-node1"`

	ConsumerBatchSize     int    `env:"CONSUMER_BATCH_SIZE" env-default:"1000"`
	ConsumerBatchInterval string `env:"CONSUMER_BATCH_INTERVAL" env-default:"500ms"`
}

func (c *Config) durationConsumerBatchInterval() time.Duration {
	r, _ := time.ParseDuration(c.ConsumerBatchInterval)
	return r
}
func main() {

	logger := logrus.New().WithField("component", "brokermain")

	c := &Config{}
	err := cleanenv.ReadEnv(c)
	if err != nil {
		logger.Fatal(err)
	}

	topic := "unique_topic_test"
	//
	//cfg := embed.NewConfig()
	//cfg.Dir = "default.etcd"
	//e, err := embed.StartEtcd(cfg)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//defer e.Close()
	//select {
	//case <-e.Server.ReadyNotify():
	//	log.Printf("Server is ready!")
	//case <-time.After(60 * time.Second):
	//	e.Server.Stop() // trigger a shutdown
	//	log.Printf("Server took too long to start!")
	//}
	//log.Fatal(<-e.Err())

	//TODO add here cluster level metadata to get the topicPartitions, Consumers and other stuff
	topicLocalMetadata := server.NewTopicLocalData(topic, c.DataDirectory, logger, uint16(c.Partitions))

	//Dejaq stuff
	ser := grpc.NewServer(
		grpc.CustomCodec(flatbuffers.FlatbuffersCodec{}),
	)
	DejaQ.RegisterBrokerServer(ser, server.NewGRPC(logger, topicLocalMetadata, c.ConsumerBatchSize, c.durationConsumerBatchInterval()))
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
	topicLocalMetadata.Close()
}
