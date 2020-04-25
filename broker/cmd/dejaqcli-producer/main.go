package main

import (
	"context"
	"fmt"
	"github.com/dejaq/prototype/common/metrics/exporter"
	"os"
	"os/signal"
	"time"

	"github.com/dejaq/prototype/common/timeline"

	"github.com/dejaq/prototype/client/satellite"
	"github.com/dejaq/prototype/client/timeline/producer"
	"github.com/dejaq/prototype/client/timeline/sync_produce"
	derror "github.com/dejaq/prototype/common/errors"
	"github.com/ilyakaznacheev/cleanenv"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	subsystemProducer = "producer"
)

type Config struct {
	OverseerSeed  string `env:"OVERSEER" env-default:"localhost:9000"`
	Topic         string `env:"TOPIC"`
	TopicBuckets  int    `env:"TOPIC_BUCKETS" env-default:"100"`
	ProducerGroup string `env:"NAME"`

	// after this duration the process wil close
	TimeoutDuration string `env:"TIMEOUT" env-default:"10s"`
	// the process will close after it sends this amount of messages
	SingleBurstEventsCount int `env:"SINGLE_BURST_EVENTS"`

	// as alternative to SINGLE_BURST_EVENTS, set these to keep writing messages indefinately
	ConstantBurstsTickDuration    string `env:"CONSTANT_TICK_DURATION"`
	ConstantBurstsTickEventsCount int    `env:"CONSTANT_TICK_COUNT"`

	// the number of events to be sent in a single GRPC call
	BatchSize int `env:"BATCH_SIZE" env-default:"3000"`
	// the size of the messages
	BodySizeBytes int `env:"BODY_SIZE" env-default:"12000"`
	// The event timestamp will be determined with a Time.Now() + Rand(-MinDelta,MaxDelta)
	//set MaxDelta = 0 to have all the events in the past
	//set MinDelta > 0 to have all of them in the future
	EventTimelineMinDelta string `env:"EVENT_TIME_MIN_DELTA" env-default:"3s"`
	EventTimelineMaxDelta string `env:"EVENT_TIME_MAX_DELTA" env-default:"0s"`

	// messages will have a deterministic ID for debuging purposes
	DeterministicEventID bool `env:"DETERMINISTIC_ID"`
	strategy             sync_produce.Strategy
}

func (c *Config) IsValid() error {
	if c.SingleBurstEventsCount > 0 && c.ConstantBurstsTickEventsCount > 0 {
		return errors.New("there can be only 1 strategy, set either SingleBurstEventsCount OR  ConstantBurstsTickEventsCount")
	}

	c.strategy = sync_produce.StrategySingleBurst
	if c.ConstantBurstsTickEventsCount > 0 {
		c.strategy = sync_produce.StrategyConstantBursts
	}

	if c.strategy == sync_produce.StrategyConstantBursts {
		if _, err := time.ParseDuration(c.ConstantBurstsTickDuration); err != nil {
			return fmt.Errorf("ConstantBurstsTickDuration provided but wrong value %s", err.Error())
		}
		if c.DeterministicEventID {
			return errors.New("deterministic IDs are not implemented for StrategyConstantBursts")
		}
	} else {
		//timeout is required only for single burst strategy
		if _, err := time.ParseDuration(c.TimeoutDuration); err != nil {
			return fmt.Errorf("timeout provided but wrong value %s", err.Error())
		}
		if c.SingleBurstEventsCount < 1 {
			return errors.New("SingleBurstEventsCount has to be > 0")
		}
	}
	if _, err := time.ParseDuration(c.EventTimelineMinDelta); err != nil {
		return fmt.Errorf("EventTimelineMinDelta provided but wrong value %s", err.Error())
	}
	if _, err := time.ParseDuration(c.EventTimelineMaxDelta); err != nil {
		return fmt.Errorf("EventTimelineMaxDelta provided but wrong value %s", err.Error())
	}
	if c.BatchSize < 1 {
		return errors.New("batch size must be >= 1")
	}
	if c.BodySizeBytes < 1 {
		return errors.New("body size must be >= 1")
	}

	if c.OverseerSeed == "" || c.Topic == "" || c.ProducerGroup == "" {
		return errors.New("topic, overseerSeed and producer groups are mandatory values")
	}
	return nil
}

func (c *Config) durationConstantBursts() time.Duration {
	r, _ := time.ParseDuration(c.ConstantBurstsTickDuration)
	return r
}
func (c *Config) durationMinDelta() time.Duration {
	r, _ := time.ParseDuration(c.EventTimelineMinDelta)
	return r
}
func (c *Config) durationMaxDelta() time.Duration {
	r, _ := time.ParseDuration(c.EventTimelineMaxDelta)
	return r
}
func (c *Config) durationTimeout() time.Duration {
	r, _ := time.ParseDuration(c.TimeoutDuration)
	return r
}

func main() {
	go exporter.SetupStandardMetricsExporter(subsystemProducer)

	logger := logrus.New().WithField("component", subsystemProducer)

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

	chief := client.NewOverseerClient()
	err = chief.CreateTimelineTopic(ctx, c.Topic, timeline.TopicSettings{
		ReplicaCount:            0,
		MaxSecondsFutureAllowed: 10,
		MaxSecondsLease:         10,
		ChecksumBodies:          false,
		MaxBodySizeBytes:        100000,
		RQSLimitPerClient:       100000,
		MinimumProtocolVersion:  0,
		MinimumDriverVersion:    0,
		BucketCount:             uint16(c.TopicBuckets),
	})
	if err != nil {
		logger.WithError(err).Fatal("failed creating topic")
		return
	}

	p := client.NewProducer(&producer.Config{
		Cluster:         "",
		Topic:           c.Topic,
		ProducerGroupID: c.ProducerGroup,
		ProducerID:      c.ProducerGroup + "_singleton",
	})
	err = p.Handshake(ctx)
	if err != nil {
		logger.WithError(err).Fatal("cannot handshake")
	}

	logger.Infof("strategy: %s", c.strategy.String())
	pc := sync_produce.SyncProduceConfig{
		Strategy:                      c.strategy,
		SingleBurstEventsCount:        c.SingleBurstEventsCount,
		ConstantBurstsTickDuration:    c.durationConstantBursts(),
		ConstantBurstsTickEventsCount: c.ConstantBurstsTickEventsCount,
		BatchSize:                     c.BatchSize,
		EventTimelineMinDelta:         c.durationMinDelta(),
		EventTimelineMaxDelta:         c.durationMaxDelta(),
		BodySizeBytes:                 c.BodySizeBytes,
		DeterministicEventID:          c.DeterministicEventID,
	}

	go func() {
		err = sync_produce.Produce(ctx, &pc, p, logger)
		if err != nil {
			switch v := err.(type) {
			case derror.MessageIDTupleList:
				for _, ve := range v {
					logger.WithError(ve.MsgError).Errorf("message ID failed %s", string(ve.MsgID))
				}
			default:
				logger.Error(err.Error())
			}
		}
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
	client.Close()
	logger.Info("Exiting, waiting 10s")
	//wait for stuff to shutdown
	time.Sleep(time.Second * 10)
}
