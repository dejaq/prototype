package sync_consume

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"time"

	dtime "github.com/dejaq/prototype/common/time"

	"github.com/dejaq/prototype/client/timeline/consumer"
	"github.com/dejaq/prototype/common/timeline"
	"github.com/prometheus/common/log"
	"go.uber.org/atomic"
)

type Average struct {
	sum int64
	n   int
}

func (a Average) Add(x int64) {
	if x <= 0 {
		return
	}
	a.sum += x
	a.n++
}
func (a Average) Get() int64 {
	if a.n > 0 {
		return a.sum / int64(a.n)
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
}

func (sc *syncconsumer) callback(lease timeline.Lease) {
	if lease.GetConsumerID() != sc.conf.Consumer.GetConsumerID() {
		log.Fatalf("server sent message for another consumer me=%s sent=%s", sc.conf.Consumer.GetConsumerID(), lease.GetConsumerID())
	}
	if !strings.Contains(lease.Message.GetID(), sc.conf.Consumer.GetTopicID()) {
		log.Fatalf("server sent message for another topic: %s sent consumerID: %s, msgID: %s, producedBy: %s",
			sc.conf.Consumer.GetTopicID(), lease.GetConsumerID(), lease.Message.GetID(), lease.Message.GetProducerGroupID())
	}
	//Process the messages
	err := sc.conf.Consumer.Delete(sc.ctx, []timeline.Message{{
		ID:          lease.Message.ID,
		TimestampMS: lease.Message.TimestampMS,
		BucketID:    lease.Message.BucketID,
		Version:     lease.Message.Version,
	}})

	sc.msgsCounter.Dec()

	if err != nil {
		log.Errorf("delete failed", err)
		return
	}

	//retrieve the time when it was created, so we can see how long it took to process it
	parts := bytes.SplitN(lease.Message.Body[:25], []byte{'|'}, 2)
	if len(parts) != 2 || len(parts[0]) == 0 {
		log.Errorf("malformed body (ts for latency not found) id=%s body=%s", lease.Message.ID, lease.Message.Body)
		return
	}

	createdMSTs, err := strconv.Atoi(string(parts[0]))
	if err != nil {
		log.Errorf("malformed lateny ts, not an int id=%s err=%v", lease.Message.ID, err)
		return
	}

	currentMSTS := dtime.TimeToMS(time.Now().UTC())
	if int64(currentMSTS) <= int64(createdMSTs)-2 {
		log.Errorf("we invented a time machine id=%s err=%v", lease.Message.ID, err)
		return
	}
	sc.avg.Add(int64(currentMSTS) - int64(createdMSTs))
}

func Consume(ctx context.Context, msgsCounter *atomic.Int64, conf *SyncConsumeConfig) (int64, error) {

	sc := &syncconsumer{
		avg:         &Average{},
		ctx:         ctx,
		msgsCounter: msgsCounter,
		conf:        conf,
	}

	conf.Consumer.Start(ctx, sc.callback)

	<-ctx.Done()
	return sc.avg.Get(), nil
}
