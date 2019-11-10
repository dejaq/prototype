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
}

type Loader struct {
	opMutex sync.Mutex
	conf    *LConfig
	myCtx   context.Context
	cancel  context.CancelFunc
	storage storage.Repository
	dealer  Dealer
	greeter *Greeter
}

func NewLoader(conf *LConfig, storage storage.Repository, dealer Dealer, greeter *Greeter) *Loader {
	return &Loader{
		conf:    conf,
		storage: storage,
		dealer:  dealer,
		opMutex: sync.Mutex{},
		greeter: greeter,
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
			case <-time.After(time.Millisecond * 15):
				thisIntervalCtx, _ := context.WithTimeout(ctx, time.Second*5)
				c.loadMessages(thisIntervalCtx)
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

func (c *Loader) loadMessages(ctx context.Context) {
	wg := sync.WaitGroup{}

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
			} else if !sentAllMessges {
				//TODO decrease the weight of this consumer, because it has more messages
				log.Printf("sent %d msgs but still has more", msgsSent)
			}
			wg.Done()
		}(tuple.C, tuple.Pipeline)
	}
	wg.Wait()

	//TODO based on the throttler of each consumer change the buckets assigned
}

//returns number of sent messages, if it sent all of them, and an error
func (c *Loader) loadOneConsumer(ctx context.Context, consumer *Consumer, limit int, consumerPipeline chan<- timeline.PushLeases) (int, bool, error) {
	sent := 0
	var hasMoreForThisBucket bool
	var pushLeaseMessages []timeline.PushLeases
	for bi := range consumer.AssignedBuckets {
		hasMoreForThisBucket = true //we presume it has
		for hasMoreForThisBucket {
			pushLeaseMessages, hasMoreForThisBucket, _ = c.storage.GetAndLease(ctx, consumer.GetTopic(), consumer.AssignedBuckets[bi], consumer.GetID(), consumer.LeaseMs, limit, dtime.TimeToMS(time.Now()))
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

type LoaderTimer struct {
}

func (t *LoaderTimer) GetNextDuration() time.Duration {
	return time.Millisecond * 15
}

func (t *LoaderTimer) Decrease() {

}

func (t *LoaderTimer) Increase() {

}
