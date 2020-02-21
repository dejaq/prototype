package sync_produce

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/prometheus/common/log"

	"github.com/dejaq/prototype/client/timeline/producer"
	dtime "github.com/dejaq/prototype/common/time"
	"github.com/dejaq/prototype/common/timeline"
	"github.com/pkg/errors"
)

type SyncProduceConfig struct {
	Count                            int
	BatchSize                        int
	Producer                         *producer.Producer
	ProduceDeltaMin, ProduceDeltaMax time.Duration
}

var (
	oneKBBody = []byte(`Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc vel urna vel ligula scelerisque luctus. Integer ultrices dui eget lectus tristique, eu dictum massa venenatis. Nulla facilisi. Nulla ex ex, commodo nec arcu a, varius aliquam enim. In ipsum arcu, ultricies eget ultricies nec, malesuada sed lorem. Morbi consectetur nibh risus, a consectetur nunc venenatis nec. Pellentesque nisl quam, suscipit sit amet quam non, hendrerit semper mi. Praesent vel pellentesque nisl, vehicula semper nunc.

Suspendisse posuere neque ac tristique tincidunt. Praesent ac ante dui. Donec ultrices a est efficitur rutrum. Nullam quis diam ut leo commodo convallis ac nec neque. Proin lobortis augue nec erat aliquam, sit amet laoreet arcu imperdiet. Cras tincidunt dolor quam, a ultrices est fringilla vitae. Ut tincidunt, nunc at ullamcorper posuere, lacus odio dictum quam, a imperdiet est odio eget dolor. Aliquam eu euismod quam. Pellentesque sit amet bibendum lectus. Phasellus consequat commodo urna in eleifend. Aenean ut ante quis magna vulputate scelerisque eu ac massa. Fusce a mauris egestas, facilisis nisi quis, malesuada elit. Curabitur malesuada erat sed justo dictum, sit amet blandit augue fermentum. Proin eros quam, tempus eu porta non, malesuada vitae est.`)
	twelveKBBody    = []byte{}
	sixtyFourKBBody = []byte{}
)

func init() {
	for i := 0; i < 64; i++ {
		if i < 12 {
			twelveKBBody = append(twelveKBBody, oneKBBody...)
		}
		sixtyFourKBBody = append(sixtyFourKBBody, oneKBBody...)
	}
}

func Produce(ctx context.Context, config *SyncProduceConfig) error {
	var t time.Time
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
		batch = batch[:0]
		return nil
	}

	for left > 0 {
		left--
		msgID := config.Count - left
		t = time.Now().UTC()
		minT := dtime.TimeToMS(t.Add(-config.ProduceDeltaMin))
		maxT := dtime.TimeToMS(t.Add(config.ProduceDeltaMax))

		bodyHeader := fmt.Sprintf("%s|BODY %s|msg_%d|", strconv.Itoa(int(dtime.TimeToMS(t))), config.Producer.GetProducerGroupID(), msgID)

		batch = append(batch, timeline.Message{
			//first part must be the the ms timestamp, so consumers can calculate the latency
			ID:   []byte(fmt.Sprintf("id %s|msg_%d | topic_%s", config.Producer.GetProducerGroupID(), msgID, config.Producer.GetTopic())),
			Body: append([]byte(bodyHeader), twelveKBBody...),
			//TimestampMS: dtime.TimeToMS(t.Add(time.Millisecond + time.Duration(msgID+200))),
			TimestampMS: minT + uint64(rand.Intn(int(maxT-minT))),
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
