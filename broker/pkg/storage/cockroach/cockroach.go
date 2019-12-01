package cockroach

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/lib/pq"
	"github.com/sirupsen/logrus"

	"github.com/bgadrian/dejaq-broker/broker/domain"
	"github.com/bgadrian/dejaq-broker/common/errors"
	"github.com/bgadrian/dejaq-broker/common/timeline"
)

var (
	stmtTopic = `
DROP TABLE IF EXISTS $TABLE;
CREATE TABLE $TABLE (
				id BYTES NOT NULL,    
				bucket_id INT NOT NULL,    
				ts INT NOT NULL,
				producer_group_id BYTES NOT NULL,
				consumer_id BYTES,
				body BYTES,
				version INT NOT NULL,
				CONSTRAINT "primary" PRIMARY KEY (id ASC),
				INDEX $TABLE_index_ts (ts ASC),
				INDEX $TABLE_index_bucket (bucket_id ASC),
				INDEX $TABLE_index_consumerID (consumer_id ASC),    
				FAMILY stable (id, bucket_id, producer_group_id, body),  
 				FAMILY mutable (ts, consumer_id, version)
		);
`
)

type CRClient struct {
	db     *sql.DB
	logger logrus.FieldLogger
}

func New(db *sql.DB, logger logrus.FieldLogger) *CRClient {
	return &CRClient{db: db, logger: logger}
}

func table(topicID string) string {
	return fmt.Sprintf("timeline_%s", topicID)
}

func (c *CRClient) CreateTopic(ctx context.Context, timelineID string) error {
	//table and column names cannot be prepared
	replaced := strings.Replace(stmtTopic, "$TABLE", table(timelineID), -1)
	_, err := c.db.ExecContext(ctx, replaced)
	if err != nil {
		return fmt.Errorf("create failed: %w", err)
	}
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (c *CRClient) Insert(ctx context.Context, timelineID []byte, messages []timeline.Message) []errors.MessageIDTuple {

	batchSize := 25
	var batch []timeline.Message

	generateRemainingErrors := func(err error) []errors.MessageIDTuple {
		result := make([]errors.MessageIDTuple, 0, len(batch)+len(messages))
		add := func(msg timeline.Message) {
			result = append(result, errors.MessageIDTuple{
				MessageID: msg.ID,
				Error: errors.Dejaror{
					Severity:         errors.SeverityError,
					Message:          "failed",
					Module:           errors.ModuleStorage,
					Operation:        "Insert",
					Kind:             0,
					Details:          nil,
					WrappedErr:       err,
					ShouldRetry:      true,
					ClientShouldSync: false,
				},
			})
		}
		for i := range batch {
			add(batch[i])
		}
		for i := range messages {
			add(messages[i])
		}
		return result
	}

	for len(messages) > 0 {
		max := min(len(messages), batchSize)
		batch = messages[:max]
		messages = messages[max:]

		//https://godoc.org/github.com/lib/pq#hdr-Bulk_imports
		txn, err := c.db.Begin()
		if err != nil {
			log.Fatal(err)
		}

		stmt, err := txn.Prepare(pq.CopyIn(table(string(timelineID)), "id", "bucket_id", "ts", "producer_group_id", "body", "version"))
		if err != nil {
			return generateRemainingErrors(err)
		}

		for i := range batch {
			_, err = stmt.Exec(batch[i].ID,
				batch[i].BucketID,
				batch[i].TimestampMS,
				batch[i].ProducerGroupID,
				batch[i].Body,
				batch[i].Version)
			if err != nil {
				log.Fatal(err)
			}
		}

		_, err = stmt.Exec()
		if err != nil {
			return generateRemainingErrors(err)
		}

		err = stmt.Close()
		if err != nil {
			return generateRemainingErrors(err)
		}

		err = txn.Commit()
		if err != nil {
			return generateRemainingErrors(err)
		}
	}
	return nil
}

func (c *CRClient) GetAndLease(ctx context.Context, timelineID []byte, buckets domain.BucketRange, consumerId string, leaseMs uint64, limit int, maxTimestamp uint64) ([]timeline.Lease, bool, []errors.MessageIDTuple) {
	return nil, false, nil
}

func (c *CRClient) Lookup(ctx context.Context, timelineID []byte, messageIDs [][]byte) ([]timeline.Message, []errors.MessageIDTuple) {
	panic("implement me")
}

func (c *CRClient) Delete(ctx context.Context, timelineID []byte, messageIDs []timeline.Message) []errors.MessageIDTuple {
	panic("implement me")
}

func (c *CRClient) CountByRange(ctx context.Context, timelineID []byte, a, b uint64) uint64 {
	panic("implement me")
}

func (c *CRClient) CountByRangeProcessing(ctx context.Context, timelineID []byte, a, b uint64) uint64 {
	panic("implement me")
}

func (c *CRClient) CountByRangeWaiting(ctx context.Context, timelineID []byte, a, b uint64) uint64 {
	panic("implement me")
}

func (c *CRClient) SelectByConsumer(ctx context.Context, timelineID []byte, consumerID []byte) []timeline.Message {
	panic("implement me")
}

func (c *CRClient) SelectByProducer(ctx context.Context, timelineID []byte, producrID []byte) []timeline.Message {
	panic("implement me")
}
