package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/dejaq/prototype/common/metrics/exporter"
	"os"
	"os/signal"
	"time"

	derror "github.com/dejaq/prototype/common/errors"

	"github.com/ilyakaznacheev/cleanenv"

	"github.com/dejaq/prototype/client/satellite"
	"github.com/dejaq/prototype/client/timeline/consumer"
	"github.com/dejaq/prototype/client/timeline/sync_consume"
	"github.com/sirupsen/logrus"
)

const (
	subsystemConsumer = "consumer"
)

type Config struct {
	// the broker address
	OverseerSeed string `env:"OVERSEER" env-default:"localhost:9000"`
	Topic        string `env:"TOPIC"`
	// this consumer instance
	ConsumerID string `env:"CONSUMER_ID"`

	// the max amount of messages to prefetch/stored in buffer
	MaxBufferSize int `env:"MAX_BUFFER_SIZE" env-default:"3000"`
	// the time to own the leases
	LeaseDuration string `env:"LEASE_DURATION" env-default:"1m"`
	// consumer will send info to broker at specific duration
	UpdatePreloadStatsTick string `env:"PRELOAD_STATS_TICK" env-default:"1s"`
	// stop after consume n messages, -1 will run continuously
	StopAfterCount int `env:"STOP_AFTER" env-default:"-1"`
	// If false the messages will be served to another consumer after this ones lease expires
	DeleteMessages          bool `env:"DELETE_MESSAGES" env-default:"true"`
	DeleteMessagesBatchSize int  `env:"DELETE_MESSAGES_BATCH_SIZE" env-default:"100"`

	// the process will close after this
	TimeoutDuration string `env:"TIMEOUT" env-default:"10s"`
	strategy        sync_consume.Strategy
}

func (c *Config) IsValid() error {
	c.strategy = sync_consume.StrategyContinuous
	if c.StopAfterCount > 0 {
		c.strategy = sync_consume.StrategyStopAfter
	}

	if c.OverseerSeed == "" || c.Topic == "" || c.ConsumerID == "" {
		return errors.New("topic, overseerSeed and consumer ID are mandatory values")
	}
	if c.MaxBufferSize < 1 {
		return errors.New("MaxBufferSize has to be > 0")
	}
	if _, err := time.ParseDuration(c.LeaseDuration); err != nil {
		return fmt.Errorf("LeaseDuration provided but wrong value %s", err.Error())
	}
	if _, err := time.ParseDuration(c.UpdatePreloadStatsTick); err != nil {
		return fmt.Errorf("UpdatePreloadStatsTick provided but wrong value %s", err.Error())
	}
	if c.TimeoutDuration != "" {
		if _, err := time.ParseDuration(c.TimeoutDuration); err != nil {
			return fmt.Errorf("timeout provided but wrong value %s", err.Error())
		}
	}
	if c.DeleteMessagesBatchSize < 1 {
		c.DeleteMessagesBatchSize = 1
	}
	if c.strategy == sync_consume.StrategyStopAfter && c.DeleteMessagesBatchSize > 1 {
		return errors.New("DELETE_MESSAGES_BATCH_SIZE must be 1 for  STOP AFTER strategy")
	}
	return nil
}

func (c *Config) durationLease() time.Duration {
	r, _ := time.ParseDuration(c.LeaseDuration)
	return r
}

func (c *Config) durationPreloadStatsTick() time.Duration {
	r, _ := time.ParseDuration(c.UpdatePreloadStatsTick)
	return r
}

func (c *Config) durationTimeout() time.Duration {
	r, _ := time.ParseDuration(c.TimeoutDuration)
	return r
}

func main() {
	go exporter.SetupStandardMetricsExporter(subsystemConsumer)

	logger := logrus.New().WithField("component", subsystemConsumer)

	c := &Config{}
	err := cleanenv.ReadEnv(c)
	if err != nil {
		logger.Fatal(err)
	}
	if err = c.IsValid(); err != nil {
		logger.Fatal(err)
	}

	ctx, shutdownEverything := context.WithCancel(context.Background())
	if c.TimeoutDuration != "" {
		ctx, _ = context.WithTimeout(ctx, c.durationTimeout())
	}

	clientConfig := satellite.Config{
		Cluster:        "",
		OverseersSeeds: []string{c.OverseerSeed},
	}
	client, err := satellite.NewClient(ctx, logger, &clientConfig)
	if err != nil {
		logger.WithError(err).Fatal("brokerClient")
	}

	waitForCtx := ctx
	if c.TimeoutDuration == "" {
		waitForCtx, _ = context.WithTimeout(ctx, time.Second*10)
	}
	if !client.WaitForConnection(waitForCtx) {
		logger.Fatal("The connection to the broker cannot be established in time.")
	}

	cons := client.NewConsumer(&consumer.Config{
		ConsumerID:             c.ConsumerID,
		Topic:                  c.Topic,
		Cluster:                "",
		MaxBufferSize:          int64(c.MaxBufferSize),
		LeaseDuration:          c.durationLease(),
		UpdatePreloadStatsTick: c.durationPreloadStatsTick(),
	})
	err = cons.Handshake(ctx)
	if err != nil {
		logger.WithError(err).Fatal("cannot handshake")
	}

	logger.Infof("strategy: %s", c.strategy.String())
	cc := sync_consume.SyncConsumeConfig{
		Strategy:        c.strategy,
		StopAfterCount:  c.StopAfterCount,
		DeleteMessages:  c.DeleteMessages,
		DecreaseCounter: nil,
		DeleteBatchSize: c.DeleteMessagesBatchSize,
	}

	go func() {
		defer shutdownEverything() //propagate trough the context

		result, err := sync_consume.Consume(ctx, logger, cons, &cc)
		if err != nil {
			// TODO check wat returns here
			switch v := err.(type) {
			case derror.MessageIDTupleList:
				for _, ve := range v {
					logger.WithError(ve.MsgError).Errorf("message ID failed %s", string(ve.MsgID))
				}
			default:
				logger.Error(err.Error())
			}
			return
		}

		logger.Infof("received: %d avg round trip %s", result.Received, result.AvgMsgLatency.String())
	}()

	go func() {
		//we need to wait for kill signal
		shutdownSignal := make(chan os.Signal, 1)
		signal.Notify(shutdownSignal)
		logger.Info("Press CTRL-C or kill the process to stop the consumer")
		// Block until any signal is received or context closed, because we do not have a Default branch
		select {
		case <-shutdownSignal:
			shutdownEverything() //propagate trough the context
			return
		case <-ctx.Done():
			return
		}
	}()

	<-ctx.Done()
	client.Close()

	// check if all components have graceful shutdown,
	// and remove 10s wait
	logger.Info("Exiting, waiting 10s")
	//wait for stuff to shutdown
	time.Sleep(time.Second * 10)
}
