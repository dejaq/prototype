package sync_consume

import (
	"context"
	"time"

	"github.com/dejaq/prototype/client/timeline/sync_produce"

	"github.com/dejaq/prototype/client/timeline/consumer"
	"github.com/dejaq/prototype/common/timeline"
	"github.com/sirupsen/logrus"
	"go.uber.org/atomic"
)

type SyncConsumeConfig struct {
	Strategy Strategy
	// Used in StrategyStopAfter strategy, no of messages
	StopAfterCount int
	DeleteMessages bool
	//Deprecated for old main
	DecreaseCounter *atomic.Int64
}

type Result struct {
	AvgMsgLatency       time.Duration
	Received            int
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

	lastTime := time.Now().UTC()
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
			err = c.Delete(ctx, []timeline.Message{{
				ID:          lease.Message.ID,
				TimestampMS: lease.Message.TimestampMS,
				BucketID:    lease.Message.BucketID,
				Version:     lease.Message.Version,
			}})
			if err != nil {
				break
			}
		}

		if r.PartialInfoReceived%100 == 0 {
			currentTime := time.Now().UTC()
			diff := currentTime.Sub(lastTime)
			lastTime = currentTime
			throughPut := (100 * time.Second) / diff
			logger.Infof("consumed messages: %d throughput/s: %d", r.Received, throughPut)
			r.PartialInfoReceived = 0
		}

		if config.DecreaseCounter != nil {
			config.DecreaseCounter.Dec()
		}

		if config.Strategy == StrategyStopAfter && config.StopAfterCount > 0 && r.Received >= config.StopAfterCount {
			break
		}
	}

	r.AvgMsgLatency = avg.Get()
	return r, err
}
