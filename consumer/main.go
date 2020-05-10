package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/dejaq/prototype/consumer/pkg"

	"github.com/dejaq/prototype/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/ilyakaznacheev/cleanenv"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

const (
	subsystemProducer = "consumer"
)

type Config struct {
	OverseerSeed string `env:"OVERSEER" env-default:"localhost:9000"`
	// number of concurrent consumers
	Workers int `env:"WORKERS_COUNT" env-default:"1"`
}

func main() {
	logger := logrus.New().WithField("component", subsystemProducer)

	c := &Config{}
	err := cleanenv.ReadEnv(c)
	if err != nil {
		logger.Fatal(err)
	}

	ctx, shutdownEverything := context.WithCancel(context.Background())

	wg := sync.WaitGroup{}
	wg.Add(c.Workers)

	for i := 0; i < c.Workers; i++ {
		go func(id int) {
			defer wg.Done()

			conn, err := newConnection(c.OverseerSeed)
			if err != nil {
				logger.WithError(err).Error("failed to connect")
				return
			}
			brokerClient := DejaQ.NewBrokerClient(conn)
			pkg.Consume(ctx, i, logger, brokerClient)
		}(i)
	}

	go func() {
		wg.Wait()
		shutdownEverything() //propagate trough the context
	}()

	go func() {
		//we need to wait for kill signal
		shutdownSignal := make(chan os.Signal, 1)
		signal.Notify(shutdownSignal)
		logger.Info("Press CTRL-C or kill the process to stop the broker")
		// Block until any signal is received or context closed, because we do not have a Default branch
		select {
		case <-shutdownSignal:
			shutdownEverything() //propagate trough the context
			return
		case <-ctx.Done():
			return
		}
	}()

	//wait until someone closed the context, is either CTRL+C or the producer finished/crashed
	<-ctx.Done()
	logger.Info("Exiting, waiting 10s")
	//wait for stuff to shutdown
	time.Sleep(time.Second * 10)
}

func newConnection(seed string) (*grpc.ClientConn, error) {
	return grpc.Dial(
		seed,
		grpc.WithInsecure(),
		grpc.WithCodec(flatbuffers.FlatbuffersCodec{}),
		grpc.WithReadBufferSize(64*1024),
		grpc.WithWriteBufferSize(64*1024),
	)
}
