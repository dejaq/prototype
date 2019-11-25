package sync_produce

import (
	"context"
	"fmt"
	"time"

	"github.com/bgadrian/dejaq-broker/client/timeline/producer"
	dtime "github.com/bgadrian/dejaq-broker/common/time"
	"github.com/bgadrian/dejaq-broker/common/timeline"
	"github.com/pkg/errors"
	"github.com/prometheus/common/log"
	"go.uber.org/atomic"
)

type SyncProduceConfig struct {
	Count     int
	BatchSize int
	Producer  *producer.Producer
}

func Produce(ctx context.Context, msgCounter *atomic.Int32, config *SyncProduceConfig) error {
	t := time.Now().UTC()
	left := config.Count
	var batch []timeline.Message

	err := config.Producer.Handshake(ctx)
	if err != nil {
		return err
	}

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := config.Producer.InsertMessages(ctx, batch)
		if err != nil {
			return errors.Wrap(err, "InsertMessages ERROR")
		}
		msgCounter.Add(int32(len(batch)))
		batch = batch[:0]
		return nil
	}

	for left > 0 {
		left--
		msgID := config.Count - left
		batch = append(batch, timeline.Message{
			ID:          []byte(fmt.Sprintf("ID %s|msg_%d", config.Producer.GetProducerGroupID(), msgID)),
			Body:        []byte(fmt.Sprintf("BODY %s|msg_%d", config.Producer.GetProducerGroupID(), msgID)),
			TimestampMS: dtime.TimeToMS(t.Add(time.Millisecond + time.Duration(msgID))),
		})
		if len(batch) >= config.BatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	log.Infof("inserted %d messages group=%s on topic=%s", config.Count, config.Producer.GetProducerGroupID(), config.Producer.GetTopic())
	return nil
}
