package coordinator

import (
	"context"
	"log"
	"sync"
	"time"

	storage "github.com/bgadrian/dejaq-broker/broker/pkg/storage/timeline"
	dtime "github.com/bgadrian/dejaq-broker/common/time"
	"github.com/bgadrian/dejaq-broker/common/timeline"
)

type LConfig struct {
	TopicDefaultNoOfBuckets uint16
	PrefetchMaxNoMsgs       uint16
	PrefetchMaxMilliseconds uint64
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
			Max:  time.Second * 2,
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

	consumersAndPipelines := c.greeter.GetAllActiveConsumers()
	consumers := make([]*Consumer, 0, len(consumersAndPipelines))
	for i := range consumersAndPipelines {
		consumers = append(consumers, consumersAndPipelines[i].C)
	}
	c.dealer.Shuffle(consumers, c.conf.TopicDefaultNoOfBuckets)

	wg.Add(len(consumers))
	newCtx, _ := context.WithDeadline(ctx, time.Now().Add(time.Second))
	for _, tuple := range consumersAndPipelines {
		go func(cons *Consumer, p chan timeline.PushLeases) {
			msgsSent, sentAllMessges, err := c.loadOneConsumer(newCtx, cons, 10, p)
			if err != nil {
				log.Println(err)
			}

			allFinishedMutex.Lock()
			if !sentAllMessges {
				allFinished = false
			}
			allFinishedMutex.Unlock()

			c.greeter.LeasesSent(cons, msgsSent)

			wg.Done()
		}(tuple.C, tuple.Pipeline)
	}
	wg.Wait()

	return !allFinished
}

//returns number of sent messages, if it sent all of them, and an error
func (c *Loader) loadOneConsumer(ctx context.Context, consumer *Consumer, limit int, consumerPipeline chan<- timeline.PushLeases) (int, bool, error) {
	sent := 0
	var hasMoreForThisBucket bool
	var pushLeaseMessages []timeline.PushLeases
	for bi := range consumer.AssignedBuckets {
		hasMoreForThisBucket = true // we presume it has
		for hasMoreForThisBucket {
			pushLeaseMessages, hasMoreForThisBucket, _ = c.storage.GetAndLease(ctx, consumer.GetTopic(), consumer.AssignedBuckets[bi], consumer.GetID(), consumer.LeaseMs, limit, dtime.TimeToMS(time.Now())+c.conf.PrefetchMaxMilliseconds)
			if len(pushLeaseMessages) == 0 {
				break
			}

			for i := range pushLeaseMessages {
				select {
				case <-ctx.Done():
					return sent, false, context.DeadlineExceeded
				default:
					consumerPipeline <- pushLeaseMessages[i]
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
