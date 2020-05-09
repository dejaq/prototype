package pkg

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/pkg/errors"

	"github.com/dejaq/prototype/grpc/DejaQ"

	"github.com/sirupsen/logrus"
)

type SyncProduceConfig struct {
	ConstantBurstsTickDuration    time.Duration
	ConstantBurstsTickEventsCount int
	BodySizeBytes                 int
	PriorityMax                   int
	Jitter                        time.Duration
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

func Produce(ctx context.Context, config *SyncProduceConfig, logger logrus.FieldLogger, brokerClient DejaQ.BrokerClient) {
	ticker := time.NewTicker(config.ConstantBurstsTickDuration)
	randSource := rand.New(rand.NewSource(time.Now().Unix()))

	for range ticker.C {
		if ctx.Err() != nil {
			ticker.Stop()
			break
		}

		//add a jitter to avoid all producers producing at the same time
		randomNs := randSource.Intn(int(config.Jitter * time.Nanosecond))
		time.Sleep(time.Duration(randomNs))

		bodies := make([][]byte, config.ConstantBurstsTickEventsCount)
		for i := range bodies {
			bodies[i] = getBodyOfSize(config.BodySizeBytes)
		}
		ackWorked, err := sendMessages(ctx, brokerClient, logger, bodies, config.PriorityMax)

		if err != nil {
			logger.WithError(err).Error("failed to send")
		} else {
			logger.Infof("got ack %v on %d msgs", ackWorked, config.ConstantBurstsTickEventsCount)
		}
	}
	return
}

// CreateMessages creates a stream and push all the messages.
func sendMessages(ctx context.Context, brokerClient DejaQ.BrokerClient, logger logrus.FieldLogger, bodies [][]byte, maxPriority int) (bool, error) {
	stream, err := brokerClient.Produce(ctx)
	if err != nil {
		logger.WithError(err).Error("failed to create pipeline")
		return false, err
	}

	var builder *flatbuffers.Builder

	builder = flatbuffers.NewBuilder(128)
	randSource := rand.New(rand.NewSource(time.Now().Unix()))

	for _, body := range bodies {
		builder.Reset()

		bodyOffset := builder.CreateByteVector(body)
		DejaQ.ProduceRequestStart(builder)
		DejaQ.ProduceRequestAddBody(builder, bodyOffset)
		DejaQ.ProduceRequestAddPriority(builder, uint16(randSource.Intn(maxPriority)))
		root := DejaQ.ProduceRequestEnd(builder)
		builder.Finish(root)
		err = stream.Send(builder)
		//TODO if err is invalid/expired sessionID do a handshake automatically
		if err != nil {
			logger.WithError(err).Error("failed to send msg")
			return false, errors.Wrap(err, "failed stream")
		}
	}

	response, err := stream.CloseAndRecv()
	if err != nil && err != io.EOF {
		return false, errors.Wrap(err, "failed to send the events")
	}
	return response.Ack(), nil
}
