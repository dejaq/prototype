package synchronization

import (
	"context"
	"github.com/bgadrian/dejaq-broker/common/timeline"
)

type Topic struct {
	timeline.Topic
}

type Repository interface {
	UpdateTopic(ctx context.Context, topic Topic) error
}
