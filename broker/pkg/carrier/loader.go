package carrier

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dejaq/prototype/broker/domain"
	storage "github.com/dejaq/prototype/broker/pkg/storage/timeline"
	"github.com/dejaq/prototype/common/protocol"
	dtime "github.com/dejaq/prototype/common/time"
	"github.com/dejaq/prototype/common/timeline"
	"github.com/sirupsen/logrus"
)

type LoaderConfig struct {
	PrefetchMaxNoMsgs       uint16
	PrefetchMaxMilliseconds uint64
	Topic                   *timeline.Topic
	Timers                  LoaderTimerConfig
	MaxBatchSize            int
}

// Loader is in charge of pushing the messages to all active consumers.
// It has an internal tick that checks the DB for available msgs.
type Loader struct {
	opMutex sync.Mutex
	conf    *LoaderConfig
	myCtx   context.Context
	cancel  context.CancelFunc
	storage storage.Repository
	dealer  Dealer
	greeter *Greeter
	timer   *loaderTimer
}

func NewLoader(conf *LoaderConfig, storage storage.Repository, dealer Dealer, greeter *Greeter) *Loader {
	return &Loader{
		conf:    conf,
		storage: storage,
		dealer:  dealer,
		opMutex: sync.Mutex{},
		greeter: greeter,
		timer:   newTimer(conf.Timers),
	}
}

func (c *Loader) Start(ctx context.Context) {
	c.opMutex.Lock()
	defer c.opMutex.Unlock()
	if c.myCtx != nil {
		return
	}
	c.myCtx, c.cancel = context.WithCancel(ctx)

	go func() {
		for {
			select {
			case <-ctx.Done():
				c.cancel = nil
				c.myCtx = nil
				return
			case <-time.After(c.timer.GetNextDuration()):
				thisIntervalCtx, _ := context.WithTimeout(ctx, time.Minute)
				allConsumersGotAllMessages := c.loadMessages(thisIntervalCtx)
				if allConsumersGotAllMessages {
					c.timer.Increase() //we can wait for more time next time
				} else {
					//make the tick faster, we need to compensate
					c.timer.Decrease()
				}
			}
		}
	}()

}

func (c *Loader) Stop() {
	c.opMutex.Lock()
	defer c.opMutex.Unlock()

	if c.cancel == nil {
		return
	}
	c.cancel()
	c.cancel = nil
	c.myCtx = nil
}

func (c *Loader) loadMessages(ctx context.Context) bool {
	wg := sync.WaitGroup{}

	allFinished := true
	allFinishedMutex := sync.Mutex{}

	hydratingConsumersAndPipelines := c.greeter.GetAllConnectedConsumersWithHydrateStatus(c.conf.Topic.ID, protocol.Hydration_Requested)
	activeConsumersAndPipelines := c.greeter.GetAllConnectedConsumersWithHydrateStatus(c.conf.Topic.ID, protocol.Hydration_Done)

	newHydrateCtx, _ := context.WithDeadline(ctx, time.Now().Add(time.Second))
	for _, tuple := range hydratingConsumersAndPipelines {
		wg.Add(1)
		go func(tuple *ConsumerPipelineTuple) {
			defer func() {
				wg.Done()
				tuple.C.SetHydrateStatus(protocol.Hydration_Done)
			}()

			tuple.C.SetHydrateStatus(protocol.Hydration_InProgress)
			msgsSent, err := c.hydrateOneConsumer(newHydrateCtx, c.conf.MaxBatchSize, tuple)
			if err != nil {
				logrus.Error(err)
				return
			}

			if msgsSent == 0 {
				return
			}

			c.greeter.LeasesSent(tuple.C, msgsSent)
		}(tuple)
	}

	activeConsumers := make([]*Consumer, 0, len(activeConsumersAndPipelines))

	if len(activeConsumersAndPipelines) == 0 {
		//logrus.Debugf("warning no active consumers found")
	} else {
		for i := range activeConsumersAndPipelines {
			activeConsumers = append(activeConsumers, activeConsumersAndPipelines[i].C)
		}

		c.dealer.Shuffle(activeConsumers, c.conf.Topic.Settings.BucketCount)

		for _, tuple := range activeConsumersAndPipelines {
			wg.Add(1)
			go func(tuple *ConsumerPipelineTuple) {
				defer wg.Done()

				msgsSent, sentAllMessages, err := c.loadOneConsumer(ctx, c.conf.MaxBatchSize, tuple)
				//fmt.Printf("Loader sent: %d\n", msgsSent)
				if err != nil {
					logrus.WithError(err).Error("loadOneConsumer failed")
					return
				}

				if msgsSent == 0 {
					return
				}

				allFinishedMutex.Lock()
				if !sentAllMessages {
					allFinished = false
				}
				allFinishedMutex.Unlock()

				c.greeter.LeasesSent(tuple.C, msgsSent)

			}(tuple)
		}
	}
	wg.Wait()

	return !allFinished
}

func (c *Loader) hydrateOneConsumer(ctx context.Context, limit int, tuple *ConsumerPipelineTuple) (int, error) {
	fullRange := domain.BucketRange{
		Start: 0,
		End:   c.conf.Topic.Settings.BucketCount - 1,
	}

	var sent int
	var err error
	var pushLeaseMessages []timeline.Lease

	defer tuple.C.AddAvailableBufferSize(-uint32(sent))
	consumerLimit := limit
	if int(tuple.C.LoadAvailableBufferSize()) < limit {
		consumerLimit = int(tuple.C.LoadAvailableBufferSize())
	}
	hasMoreForThisBucket := true // we presume it has
	for hasMoreForThisBucket {
		pushLeaseMessages, hasMoreForThisBucket, err = c.storage.SelectByConsumer(ctx, tuple.C.GetTopicAsBytes(), tuple.C.GetIDAsBytes(), fullRange, consumerLimit, dtime.TimeToMS(time.Now())+c.conf.PrefetchMaxMilliseconds)
		if err != nil {
			return sent, err
		}
		if len(pushLeaseMessages) == 0 {
			return sent, nil
		}

		batchSent, err := pushLeasesToPipeline(ctx, pushLeaseMessages, tuple)
		if err != nil {
			return sent, err
		}
		sent += batchSent
	}
	return sent, nil
}

//returns number of sent messages, if it sent all of them, and an error
func (c *Loader) loadOneConsumer(ctx context.Context, limit int, tuple *ConsumerPipelineTuple) (int, bool, error) {
	var sent int
	var err error
	var hasMoreForThisBucket bool
	var pushLeaseMessages []timeline.Lease
	for bi := range tuple.C.GetAssignedBuckets() {
		hasMoreForThisBucket = true // we presume it has
		for hasMoreForThisBucket {
			assignedBuckets := tuple.C.GetAssignedBuckets()
			consumerLimit := limit
			if int(tuple.C.LoadAvailableBufferSize()) < limit {
				consumerLimit = int(tuple.C.LoadAvailableBufferSize())
			}
			pushLeaseMessages, hasMoreForThisBucket, err = c.storage.GetAndLease(
				ctx,
				tuple.C.GetTopicAsBytes(),
				assignedBuckets[bi],
				tuple.C.GetIDAsBytes(),
				tuple.C.GetLeaseMs(),
				consumerLimit,
				dtime.TimeToMS(time.Now()),
				dtime.TimeToMS(time.Now())+c.conf.PrefetchMaxMilliseconds)

			if err != nil {
				return sent, false, err
			}
			if len(pushLeaseMessages) == 0 {
				break
			}

			batchSent, err := pushLeasesToPipeline(ctx, pushLeaseMessages, tuple)
			sent += batchSent
			if err != nil {
				return sent, false, err
			}
			tuple.C.AddAvailableBufferSize(-uint32(len(pushLeaseMessages)))
		}
	}
	return sent, true, nil
}

func pushLeasesToPipeline(ctx context.Context, pushLeaseMessages []timeline.Lease, tuple *ConsumerPipelineTuple) (int, error) {
	sent := 0
	for i := range pushLeaseMessages {
		select {
		case <-ctx.Done():
			return sent, fmt.Errorf("loadOneConsumer timed out for consumer: %s on Topic: %s %w", tuple.C.GetID(), tuple.C.GetTopic(), context.DeadlineExceeded)
		case <-tuple.Connected:
			return sent, fmt.Errorf("client d/c during a load: %s", tuple.C.GetID())
		case tuple.Pipeline <- pushLeaseMessages[i]:
			//logrus.Infof("sent msgID: %s for consumerID: %s on Topic: %s", pushLeaseMessages[i].Message.GetID(), consumer.GetID(), consumer.GetTopic())
			sent++
		}
	}
	return sent, nil
}
