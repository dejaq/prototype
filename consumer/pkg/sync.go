package pkg

import (
	"context"
	"io"

	"github.com/dejaq/prototype/grpc/DejaQ"
	"github.com/sirupsen/logrus"
)

func Consume(ctx context.Context, logger logrus.FieldLogger, brokerClient DejaQ.BrokerClient) {
	logger.Info("Start consume")

	stream, err := brokerClient.Consume(ctx)
	if err != nil {
		logger.WithError(err).Fatal("failed to create pipeline")
	}

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.WithError(err).Fatal("read from stream error")
		}

		if req == nil {
			logger.Fatal("nil request")
		}

		logger.Info(string(req.BodyBytes()))
	}

}
