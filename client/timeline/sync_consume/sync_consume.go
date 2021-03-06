package sync_consume

import (
	"context"
	"github.com/dejaq/prototype/common/metrics/exporter"
	dtime "github.com/dejaq/prototype/common/time"
	"github.com/prometheus/client_golang/prometheus"
	"time"

	"github.com/dejaq/prototype/client/timeline/sync_produce"

	"github.com/dejaq/prototype/client/timeline/consumer"
	"github.com/dejaq/prototype/common/timeline"
	"github.com/sirupsen/logrus"
	"go.uber.org/atomic"
)

var (
	metricTopicLatency = exporter.GetConsumerSummary("topic_message_latency", []string{"operation", "topic"})
)

type SyncConsumeConfig struct {
	Strategy Strategy
	// Used in StrategyStopAfter strategy, no of messages
	StopAfterCount int
	DeleteMessages bool
	//Deprecated for old main
	DecreaseCounter *atomic.Int64
	DeleteBatchSize int
}

type Result struct {
	AvgMsgLatency       time.Duration
	Received            int
	Deleted             int
	PartialInfoReceived int
}

//go:generate stringer -type=Strategy
type Strategy uint8

const (
	// Stop after consuming X messages
	StrategyStopAfter Strategy = iota
	// Do not stop, never
	StrategyContinuous
)

func Consume(ctx context.Context, logger logrus.FieldLogger, c *consumer.Consumer, config *SyncConsumeConfig) (Result, error) {
	avg := Average{}
	r := Result{}
	deleteBatcher := batcher{consumer: c, maxBatchSize: config.DeleteBatchSize}
	var deleted int

	handshakeAndStart := func() error {
		err := c.Handshake(ctx)
		if err != nil {
			return err
		}
		return c.Start()
	}

	err := handshakeAndStart()
	if err != nil {
		return r, err
	}
	defer c.Stop()
	var lease timeline.Lease

	for {
		lease, err = c.ReadLease(ctx)
		if err != nil {
			if err == consumer.ErrMissingHandshake {
				//try only once to reconnect
				err = handshakeAndStart()
				if err == nil {
					//successfull, we're back in business
					continue
				}
			}
			break
		}

		//process the message
		r.Received++
		r.PartialInfoReceived++

		latency, merr := sync_produce.ExtractLatencyFromBody(lease.Message.Body)
		if merr != nil {
			logger.WithError(err).Errorf("latency failed for msgID=%s", lease.Message.GetID())
		} else {
			avg.Add(latency)
		}

		if config.DeleteMessages {
			deleted, err = deleteBatcher.delete(ctx, lease)
			r.Deleted += deleted
			if err != nil {
				break
			}
		}

		if r.PartialInfoReceived%10000 == 0 {
			logger.Infof("consumed messages: %d avg latency: %s removed: %d", r.Received, avg.Get().String(), r.Deleted)
			r.PartialInfoReceived = 0
		}

		metricTopicLatency.With(prometheus.Labels{"operation": "delete", "topic": c.GetTopicID()}).Observe(float64(dtime.GetLatencyMS(lease.Message.TimestampMS)))

		if config.DecreaseCounter != nil {
			if config.DeleteMessages {
				config.DecreaseCounter.Sub(int64(deleted))
			} else {
				config.DecreaseCounter.Dec()
			}
		}

		if config.Strategy == StrategyStopAfter && config.StopAfterCount > 0 && r.Received >= config.StopAfterCount {
			break
		}
	}

	//if we are finished and some events are still in batch
	if err == nil && config.DeleteMessages {
		deleted, err = deleteBatcher.flush(ctx)
		if err != nil {
			logger.WithError(err).Error("delete failed")
		}
		r.Deleted += deleted
		if config.DeleteMessages {
			config.DecreaseCounter.Sub(int64(deleted))
		}
	}

	r.AvgMsgLatency = avg.Get()
	return r, err
}
