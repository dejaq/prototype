package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/dejaq/prototype/producer/pkg"

	"github.com/dejaq/prototype/grpc/DejaQ"

	flatbuffers "github.com/google/flatbuffers/go"
	"google.golang.org/grpc"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/sirupsen/logrus"
)

const (
	subsystemProducer = "producer"
)

type Config struct {
	OverseerSeed                  string `env:"OVERSEER" env-default:"localhost:9000"`
	ConstantBurstsTickDuration    string `env:"CONSTANT_TICK_DURATION" env-default:"1s"`
	ConstantBurstsTickEventsCount int    `env:"CONSTANT_TICK_COUNT" env-default:"1000"`

	// the size of the messages
	BodySizeBytes int `env:"BODY_SIZE" env-default:"12000"`
	// The event timestamp will be determined with a Time.Now() + Rand(-MinDelta,MaxDelta)

	// number of concurrent consumers
	Workers int `env:"WORKERS_COUNT" env-default:"1"`
}

func (c *Config) durationConstantBursts() time.Duration {
	r, _ := time.ParseDuration(c.ConstantBurstsTickDuration)
	return r
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
			pkg.Produce(ctx, &pkg.SyncProduceConfig{
				ConstantBurstsTickDuration:    c.durationConstantBursts(),
				ConstantBurstsTickEventsCount: c.ConstantBurstsTickEventsCount,
				BodySizeBytes:                 c.BodySizeBytes,
			}, logger, brokerClient)

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
	return grpc.Dial(seed,
		grpc.WithInsecure(),
		grpc.WithCodec(flatbuffers.FlatbuffersCodec{}),
		grpc.WithReadBufferSize(64*1024),
		grpc.WithWriteBufferSize(64*1024))
}
