package coordinator

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	storage "github.com/bgadrian/dejaq-broker/broker/pkg/storage/timeline"
	"github.com/bgadrian/dejaq-broker/common/protocol"
	dtime "github.com/bgadrian/dejaq-broker/common/time"
	"github.com/bgadrian/dejaq-broker/common/timeline"
)

type LConfig struct {
	PrefetchMaxNoMsgs       uint16
	PrefetchMaxMilliseconds uint64
	Topic                   *timeline.Topic
}

type Loader struct {
	opMutex sync.Mutex
	conf    *LConfig
	myCtx   context.Context
	cancel  context.CancelFunc
	storage storage.Repository
	dealer  Dealer
	greeter *Greeter
	timer   *LoaderTimer
}

func NewLoader(conf *LConfig, storage storage.Repository, dealer Dealer, greeter *Greeter) *Loader {
	return &Loader{
		conf:    conf,
		storage: storage,
		dealer:  dealer,
		opMutex: sync.Mutex{},
		greeter: greeter,
		//TODO move the min to the NewLoader as parameter, each Storage would want different setting
		timer: NewTimer(&LoaderTimerConfig{
			Min:  time.Millisecond * 5,
			Max:  time.Millisecond * 300,
			Step: time.Millisecond * 25,
		}),
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
		//TODO replace this with a smarter time interval
		for {
			select {
			case <-ctx.Done():
				c.cancel = nil
				c.myCtx = nil
				return
			case <-time.After(c.timer.GetNextDuration()):
				thisIntervalCtx, _ := context.WithTimeout(ctx, time.Second*5)
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

	hydratingConsumersAndPipelines := c.greeter.GetAllConsumersWithHydrateStatus(c.conf.Topic.ID, protocol.Hydration_Requested)
	activeConsumersAndPipelines := c.greeter.GetAllConsumersWithHydrateStatus(c.conf.Topic.ID, protocol.Hydration_Done)

	newHydrateCtx, _ := context.WithDeadline(ctx, time.Now().Add(time.Second))
	for _, tuple := range hydratingConsumersAndPipelines {
		wg.Add(1)
		go func(cons *Consumer, p chan timeline.Lease) {
			defer func() {
				wg.Done()
				cons.HydrateStatus = protocol.Hydration_Done
			}()

			cons.HydrateStatus = protocol.Hydration_InProgress
			msgsSent, err := c.hydrateOneConsumer(newHydrateCtx, cons, p)
			if err != nil {
				logrus.Error(err)
				return
			}

			if msgsSent == 0 {
				return
			}

			c.greeter.LeasesSent(cons, msgsSent)
		}(tuple.C, tuple.Pipeline)
	}

	activeConsumers := make([]*Consumer, 0, len(activeConsumersAndPipelines))

	if len(activeConsumersAndPipelines) == 0 {
		//logrus.Debugf("warning no active consumers found")
	} else {
		for i := range activeConsumersAndPipelines {
			activeConsumers = append(activeConsumers, activeConsumersAndPipelines[i].C)
		}

		c.dealer.Shuffle(activeConsumers, c.conf.Topic.Settings.BucketCount)

		newCtx, _ := context.WithDeadline(ctx, time.Now().Add(time.Second))
		for _, tuple := range activeConsumersAndPipelines {
			wg.Add(1)
			go func(cons *Consumer, p chan timeline.Lease) {
				defer wg.Done()

				msgsSent, sentAllMessages, err := c.loadOneConsumer(newCtx, cons, 100, p)
				if err != nil {
					log.Println(err)
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

				c.greeter.LeasesSent(cons, msgsSent)

			}(tuple.C, tuple.Pipeline)
		}
	}
	wg.Wait()

	return !allFinished
}

func (c *Loader) hydrateOneConsumer(ctx context.Context, consumer *Consumer, consumerPipeline chan<- timeline.Lease) (int, error) {
	return 0, nil
}

//returns number of sent messages, if it sent all of them, and an error
func (c *Loader) loadOneConsumer(ctx context.Context, consumer *Consumer, limit int, consumerPipeline chan<- timeline.Lease) (int, bool, error) {
	sent := 0
	var hasMoreForThisBucket bool
	var pushLeaseMessages []timeline.Lease
	for bi := range consumer.AssignedBuckets {
		hasMoreForThisBucket = true // we presume it has
		for hasMoreForThisBucket {
			pushLeaseMessages, hasMoreForThisBucket, _ = c.storage.GetAndLease(ctx, consumer.GetTopic(), consumer.AssignedBuckets[bi], consumer.GetID(), consumer.LeaseMs, limit, dtime.TimeToMS(time.Now())+c.conf.PrefetchMaxMilliseconds)
			if len(pushLeaseMessages) == 0 {
				break
			}

			for i := range pushLeaseMessages {
				if pushLeaseMessages[i].Message.GetID() == "" {
					logrus.Fatalf("storage returned empty msgID")
				}
				select {
				case <-ctx.Done():
					logrus.Infof("loadOneConsumer timed out for consumer: %s on topic: %s", consumer.ID, consumer.Topic)
					return sent, false, context.DeadlineExceeded
				case consumerPipeline <- pushLeaseMessages[i]:
					//logrus.Infof("sent msgID: %s for consumerID: %s on topic: %s", pushLeaseMessages[i].Message.GetID(), consumer.GetID(), consumer.GetTopic())
					sent++
				}
			}
		}
	}
	return sent, true, nil
}

type LoaderTimerConfig struct {
	Min, Max time.Duration
	Step     time.Duration
}
type LoaderTimer struct {
	conf    *LoaderTimerConfig
	Current time.Duration
}

func NewTimer(conf *LoaderTimerConfig) *LoaderTimer {
	return &LoaderTimer{
		conf:    conf,
		Current: conf.Min,
	}
}

func (t *LoaderTimer) GetNextDuration() time.Duration {
	return t.Current
}

func (t *LoaderTimer) Decrease() {
	t.Current = t.Current - t.conf.Step
	if t.Current < t.conf.Min {
		t.Current = t.conf.Min
	}
}

func (t *LoaderTimer) Increase() {
	t.Current = t.Current + t.conf.Step
	if t.Current > t.conf.Max {
		t.Current = t.conf.Max
	}
}
