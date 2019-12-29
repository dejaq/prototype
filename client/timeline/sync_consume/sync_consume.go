package sync_consume

import (
	"context"
	"strings"

	"github.com/dejaq/prototype/client/timeline/consumer"
	"github.com/dejaq/prototype/common/timeline"
	"github.com/prometheus/common/log"
	"go.uber.org/atomic"
)

type SyncConsumeConfig struct {
	Consumer *consumer.Consumer
}

func Consume(ctx context.Context, msgsCounter *atomic.Int32, conf *SyncConsumeConfig) error {
	conf.Consumer.Start(ctx, func(lease timeline.Lease) {
		if lease.GetConsumerID() != conf.Consumer.GetConsumerID() {
			log.Fatalf("server sent message for another consumer me=%s sent=%s", conf.Consumer.GetConsumerID(), lease.GetConsumerID())
		}
		if !strings.Contains(lease.Message.GetID(), conf.Consumer.GetTopicID()) {
			log.Fatalf("server sent message for another topic: %s sent consumerID: %s, msgID: %s, producedBy: %s",
				conf.Consumer.GetTopicID(), lease.GetConsumerID(), lease.Message.GetID(), lease.Message.GetProducerGroupID())
		}
		//Process the messages
		msgsCounter.Dec()
		err := conf.Consumer.Delete(ctx, []timeline.Message{{
			ID:          lease.Message.ID,
			TimestampMS: lease.Message.TimestampMS,
			BucketID:    lease.Message.BucketID,
			Version:     lease.Message.Version,
		}})
		if err != nil {
			log.Errorf("delete failed", err)
		}
		//logrus.Printf("received message id='%s' body='%s' from bucket=%d\n", lease.Message.id, string(lease.Message.Body), lease.Message.BucketID)
	})

	//log.Info("consumer handshake success")

	for {
		select {
		case <-ctx.Done():
			return nil
		}
	}
}
