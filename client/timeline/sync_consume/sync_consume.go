package sync_consume

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/dejaq/prototype/client/timeline/consumer"
	"github.com/dejaq/prototype/common/timeline"
	"go.uber.org/atomic"
)

type Average struct {
	sum int64
	n   int
}

func (a *Average) Add(x int64) {
	if x <= 0 {
		return
	}
	a.sum += x
	a.n++
}
func (a *Average) Get() float64 {
	if a.n > 0 {
		return float64(a.sum) / float64(a.n)
	}
	return 0
}

type SyncConsumeConfig struct {
	Consumer *consumer.Consumer
}

type syncconsumer struct {
	avg         *Average
	ctx         context.Context
	msgsCounter *atomic.Int64
	conf        *SyncConsumeConfig
	logger      logrus.FieldLogger
}

func (sc *syncconsumer) callback(lease timeline.Lease) {
	if lease.GetConsumerID() != sc.conf.Consumer.GetConsumerID() {
		sc.logger.Fatalf("server sent message for another consumer me=%s sent=%s", sc.conf.Consumer.GetConsumerID(), lease.GetConsumerID())
	}
	if !strings.Contains(lease.Message.GetID(), sc.conf.Consumer.GetTopicID()) {
		sc.logger.Fatalf("server sent message for another topic: %s sent consumerID: %s, msgID: %s, producedBy: %s",
			sc.conf.Consumer.GetTopicID(), lease.GetConsumerID(), lease.Message.GetID(), lease.Message.GetProducerGroupID())
	}
	sc.msgsCounter.Dec()

	//Process the messages
	err := sc.conf.Consumer.Delete(sc.ctx, []timeline.Message{{
		ID:          lease.Message.ID,
		TimestampMS: lease.Message.TimestampMS,
		BucketID:    lease.Message.BucketID,
		Version:     lease.Message.Version,
	}})

	if err != nil {
		sc.logger.WithError(err).Error("delete failed")
		return
	}

	//retrieve the time when it was created, so we can see how long it took to process it
	parts := bytes.SplitN(lease.Message.Body[:25], []byte{'|'}, 2)
	if len(parts) != 2 || len(parts[0]) == 0 {
		sc.logger.Errorf("malformed body (ts for latency not found) id=%s body=%s", lease.Message.ID, lease.Message.Body)
		return
	}

	createdNs, err := strconv.Atoi(string(parts[0]))
	if err != nil {
		sc.logger.Errorf("malformed lateny ts, not an int id=%s err=%v", lease.Message.ID, err)
		return
	}

	currentNs := time.Now().UTC().UnixNano()
	if currentNs <= int64(createdNs) {
		sc.logger.Errorf("we invented a time machine id=%s err=%v", lease.Message.ID, err)
		return
	}
	sc.avg.Add(currentNs - int64(createdNs))
}

func Consume(ctx context.Context, msgsCounter *atomic.Int64, conf *SyncConsumeConfig, logger logrus.FieldLogger) (float64, error) {
	sc := &syncconsumer{
		avg:         &Average{},
		ctx:         ctx,
		msgsCounter: msgsCounter,
		conf:        conf,
		logger:      logger,
	}

	handshakeAndStart := func() error {
		err := conf.Consumer.Handshake(ctx)
		if err != nil {
			return err
		}
		return conf.Consumer.Start()
	}

	err := handshakeAndStart()
	if err != nil {
		return 0, err
	}
	defer conf.Consumer.Stop()

	for {
		msg, err := conf.Consumer.ReadLease(ctx)
		if err != nil {
			if err == consumer.ErrMissingHandshake {
				//try only once to reconnect
				err = handshakeAndStart()
				if err == nil {
					//successfull, we're back in business
					continue
				}
			}

			//this is not an error since we consumed all messages
			if strings.Contains(err.Error(), context.Canceled.Error()) {
				err = nil
			}
			return sc.avg.Get(), err
		}
		sc.callback(msg)
	}
}
