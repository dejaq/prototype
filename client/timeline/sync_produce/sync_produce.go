package sync_produce

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/dejaq/prototype/client/timeline/producer"
	dtime "github.com/dejaq/prototype/common/time"
	"github.com/dejaq/prototype/common/timeline"
	"github.com/pkg/errors"
	"github.com/prometheus/common/log"
)

type Strategy uint8

const (
	// Close the producer after SingleBurstEventsCount is sent
	StrategySingleBurst Strategy = iota
	// Keeps sending events each tick until the context is closed
	StrategyConstantBursts
)

type SyncProduceConfig struct {
	Strategy Strategy
	// After these amount of messages are sent the producer will close
	SingleBurstEventsCount        int
	ConstantBurstsTickDuration    time.Duration
	ConstantBurstsTickEventsCount int
	BatchSize                     int
	BodySizeBytes                 int
	// The event timestamp will be determined with a Time.Now() + Rand(-MinDelta,MaxDelta)
	EventTimelineMinDelta,
	EventTimelineMaxDelta time.Duration
	DeterministicEventID bool
}

var (
	oneKBBody = []byte(`Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc vel urna vel ligula scelerisque luctus. Integer ultrices dui eget lectus tristique, eu dictum massa venenatis. Nulla facilisi. Nulla ex ex, commodo nec arcu a, varius aliquam enim. In ipsum arcu, ultricies eget ultricies nec, malesuada sed lorem. Morbi consectetur nibh risus, a consectetur nunc venenatis nec. Pellentesque nisl quam, suscipit sit amet quam non, hendrerit semper mi. Praesent vel pellentesque nisl, vehicula semper nunc.

Suspendisse posuere neque ac tristique tincidunt. Praesent ac ante dui. Donec ultrices a est efficitur rutrum. Nullam quis diam ut leo commodo convallis ac nec neque. Proin lobortis augue nec erat aliquam, sit amet laoreet arcu imperdiet. Cras tincidunt dolor quam, a ultrices est fringilla vitae. Ut tincidunt, nunc at ullamcorper posuere, lacus odio dictum quam, a imperdiet est odio eget dolor. Aliquam eu euismod quam. Pellentesque sit amet bibendum lectus. Phasellus consequat commodo urna in eleifend. Aenean ut ante quis magna vulputate scelerisque eu ac massa. Fusce a mauris egestas, facilisis nisi quis, malesuada elit. Curabitur malesuada erat sed justo dictum, sit amet blandit augue fermentum. Proin eros quam, tempus eu porta non, malesuada vitae est.`)
)

func getBodyOfSize(size int) []byte {
	b := bytes.Buffer{}
	b.Grow(size)
	offset := 0
	for i := 0; i < size; i++ {
		b.WriteByte(oneKBBody[offset])
		offset++
		if offset >= len(oneKBBody) {
			offset = 0
		}
	}
	return b.Bytes()
}

func Produce(ctx context.Context, config *SyncProduceConfig, p *producer.Producer) error {
	switch config.Strategy {
	case StrategySingleBurst:
		return singleBurst(ctx, config, p, config.SingleBurstEventsCount)
	case StrategyConstantBursts:
		return constantBursts(ctx, config, p)
	default:
		return errors.New("unknown strategy")
	}
}

func constantBursts(ctx context.Context, config *SyncProduceConfig, p *producer.Producer) error {
	sent := uint64(0)
	ticker := time.NewTicker(config.ConstantBurstsTickDuration)
	for range ticker.C {
		if ctx.Err() != nil {
			ticker.Stop()
			break
		}
		err := singleBurst(ctx, config, p, config.ConstantBurstsTickEventsCount)
		if err != nil {
			log.Error(err)
		}
	}
	log.Infof("inserted %d messages group=%s on topic=%s", sent, p.GetProducerGroupID(), p.GetTopic())
	return nil
}

func singleBurst(ctx context.Context, config *SyncProduceConfig, p *producer.Producer, toSendCount int) error {
	left := toSendCount
	var batch []timeline.Message

	body := getBodyOfSize(config.BodySizeBytes)
	var msgID int

	for left > 0 {
		left--
		if config.DeterministicEventID {
			msgID = toSendCount - left
		} else {
			msgID = rand.Int()
		}

		batch = append(batch, newMsg(p, msgID, body, config))
		if len(batch) >= config.BatchSize {
			if err := flush(ctx, batch, p); err != nil {
				return err
			}
		}
	}
	if err := flush(ctx, batch, p); err != nil {
		return err
	}
	log.Infof("inserted %d messages group=%s on topic=%s", toSendCount, p.GetProducerGroupID(), p.GetTopic())
	return nil
}

func newMsg(p *producer.Producer, msgID int, body []byte, config *SyncProduceConfig) timeline.Message {
	t := time.Now().UTC()
	minT := dtime.TimeToMS(t.Add(-config.EventTimelineMinDelta))
	maxT := dtime.TimeToMS(t.Add(config.EventTimelineMaxDelta))
	bodyHeader := fmt.Sprintf("%s|BODY %s|msg_%d|", strconv.FormatInt(t.UnixNano(), 10), p.GetProducerGroupID(), msgID)
	return timeline.Message{
		//first part must be the the ms timestamp, so consumers can calculate the latency
		ID:   []byte(fmt.Sprintf("id %s|msg_%d | topic_%s", p.GetProducerGroupID(), msgID, p.GetTopic())),
		Body: append([]byte(bodyHeader), body...),
		//TimestampMS: dtime.TimeToMS(t.Add(time.Millisecond + time.Duration(msgID+200))),
		TimestampMS: minT + uint64(rand.Intn(int(maxT-minT))),
	}
}

func flush(ctx context.Context, batch []timeline.Message, p *producer.Producer) error {
	if len(batch) == 0 {
		return nil
	}
	err := p.InsertMessages(ctx, batch)
	if err != nil {
		return errors.Wrap(err, "InsertMessages ERROR")
	}
	batch = batch[:0]
	return nil
}
