package main

import (
	"flag"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/dejaq/prototype/broker/server"
	"github.com/dejaq/prototype/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/ilyakaznacheev/cleanenv"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/v3/raft/raftpb"
	"google.golang.org/grpc"
)

type Config struct {
	//default listens on all interfaces, this is standard for containers
	BrokerBindingAddress string `env:"DEJAQ_ADDRESS" env-default:"127.0.0.1:9000"`
	NodeID               int    `env:"NODE_ID" env-default:"1"`
	Partitions           int    `env:"PARTITIONS" env-default:"3"`
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

	//RAFT
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	proposeC := make(chan string)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	//TODO implement snapshot to allow full recovery of a node or new node start
	getSnapshot := func() ([]byte, error) { return nil, nil }
	commitC, errorC, snapshotterReady := server.NewRaftNode(c.NodeID, strings.Split(*cluster, ","), *join, getSnapshot, proposeC, confChangeC)

	kvs = newKVStore(<-snapshotterReady, proposeC, commitC, errorC)

	//END raft

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
