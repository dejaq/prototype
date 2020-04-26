package sync_consume

import (
	"context"

	"github.com/dejaq/prototype/client/timeline/consumer"
	"github.com/dejaq/prototype/common/timeline"
)

type batcher struct {
	consumer     *consumer.Consumer
	maxBatchSize int
	buffer       []timeline.Lease
}

func (b *batcher) delete(ctx context.Context, lease timeline.Lease) (int, error) {
	if b.maxBatchSize <= 1 {
		err := b.consumer.Delete(ctx, []timeline.Lease{lease})
		if err != nil {
			return 0, err
		}
		return 1, nil
	}

	b.buffer = append(b.buffer, lease)

	if len(b.buffer) <= b.maxBatchSize {
		return 0, nil
	}

	//time to flush!
	return b.flush(ctx)
}

func (b *batcher) flush(ctx context.Context) (int, error) {
	err := b.consumer.Delete(ctx, b.buffer)
	if err != nil {
		//TODO check if the error is a list, maybe some were actually removed
		return 0, err
	}
	deleted := len(b.buffer)
	b.buffer = b.buffer[:0]

	return deleted, nil
}
