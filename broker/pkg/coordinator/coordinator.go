package coordinator

import (
	"context"
	"time"

	"github.com/bgadrian/dejaq-broker/common/errors"

	"github.com/bgadrian/dejaq-broker/common/timeline"

	storage "github.com/bgadrian/dejaq-broker/broker/pkg/storage/timeline"
)

type Coordinator struct {
	storage storage.Repository
	ticker  *time.Ticker
}

func NewCoordinator(ctx context.Context, timelineStorage storage.Repository, tick time.Duration, server *GRPCServer) *Coordinator {
	l := Coordinator{
		storage: timelineStorage,
		ticker:  time.NewTicker(tick),
	}

	server.InnerServer.listeners = &GRPCListeners{
		TimelineCreateMessagesListener: l.listenerTimelineCreateMessages,
	}

	go func() {
		for {
			select {
			case <-l.ticker.C:
				available, _, _ := timelineStorage.Select(ctx, nil, nil, 10, uint64(time.Now().UTC().Unix()))
				if len(available) == 0 {
					continue
				}
				toSend := make([]timeline.PushLeases, len(available))
				for i := range available {
					toSend[i] = timeline.PushLeases{
						ExpirationTimestampMS: uint64(time.Now().UTC().Unix()) + 120,
						ConsumerID:            []byte("42"),
						Message:               timeline.NewLeaseMessage(available[i]),
					}
				}
				err := server.pushMessagesToConsumer(ctx, toSend)
				if err != nil {
					//cancel the leases!
				}
			case <-ctx.Done():
				l.ticker.Stop()
				return
			}
		}
	}()

	return &l
}

func (c *Coordinator) listenerTimelineCreateMessages(ctx context.Context, msgs []timeline.Message) []errors.MessageIDTuple {
	return c.storage.Insert(ctx, nil, msgs)
}
