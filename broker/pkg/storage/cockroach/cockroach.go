package cockroach

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/bgadrian/dejaq-broker/broker/domain"
	"github.com/bgadrian/dejaq-broker/common/errors"
	dtime "github.com/bgadrian/dejaq-broker/common/time"
	"github.com/bgadrian/dejaq-broker/common/timeline"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

var (
	stmtTopic = `
DROP TABLE IF EXISTS $TABLE;
CREATE TABLE $TABLE (
	id BYTES NOT NULL,
	timeline INT NOT NULL,
	bucket_id INT NOT NULL,    
	ts INT NOT NULL,
	producer_group_id BYTES NOT NULL,
	consumer_id BYTES,
	body_id BYTES NOT NULL UNIQUE,
	version INT NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (id ASC),
	INDEX $TABLE_index_ts (ts ASC),
	INDEX $TABLE_index_bucket (bucket_id ASC),
	INDEX $TABLE_index_consumerID (consumer_id ASC),    
	FAMILY stable (id, ts, bucket_id, producer_group_id, body_id),  
 	FAMILY mutable (timeline, consumer_id, version)
		);
DROP TABLE IF EXISTS $TABLEBODIES;
CREATE TABLE $TABLEBODIES (
	id BYTES NOT NULL,
	body BYTES,
	CONSTRAINT "primary" PRIMARY KEY (id ASC),
	CONSTRAINT $TABLEBODIES_fk FOREIGN KEY (id) REFERENCES $TABLE (body_id) ON DELETE CASCADE
);

`
) //https://www.cockroachlabs.com/docs/stable/foreign-key.html

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
func tableBodies(topicID string) string {
	return fmt.Sprintf("timeline_%s_bodies", topicID)
}

func (c *CRClient) CreateTopic(ctx context.Context, timelineID string) error {
	//table and column names cannot be prepared
	replaced := strings.Replace(stmtTopic, "$TABLE", table(timelineID), -1)
	replaced = strings.Replace(replaced, "$TABLEBODIES", tableBodies(timelineID), -1)
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
	result := make([]errors.MessageIDTuple, 0)

	addFailedBatch := func(err error) {
		for i := range batch {
			result = append(result, errors.MessageIDTuple{
				MessageID: batch[i].ID,
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

		stmt, err := txn.Prepare(pq.CopyIn(table(string(timelineID)),
			"id",
			"bucket_id",
			"ts",
			"timeline",
			"producer_group_id",
			"body_id",
			"version"))
		if err != nil {
			addFailedBatch(err)
			continue
		}

		stmtBodies, err := txn.Prepare(pq.CopyIn(tableBodies(string(timelineID)), "id", "body"))
		if err != nil {
			addFailedBatch(err)
			continue
		}
		var failedSt bool
		for i := range batch {
			_, err = stmt.Exec(
				batch[i].ID,
				batch[i].BucketID,
				batch[i].TimestampMS,
				batch[i].TimestampMS,
				batch[i].ProducerGroupID,
				batch[i].ID, //body_id same as msg_id
				batch[i].Version)
			if err != nil {
				failedSt = true
				break
			}
			_, err = stmtBodies.Exec(batch[i].ID,
				batch[i].Body)
			if err != nil {
				failedSt = true
				break
			}
		}

		if failedSt {
			addFailedBatch(err)
			continue
		}
		_, err = stmt.Exec()
		if err != nil {
			addFailedBatch(err)
			continue
		}

		err = stmt.Close()
		if err != nil {
			addFailedBatch(err)
			continue
		}

		err = txn.Commit()
		if err != nil {
			addFailedBatch(err)
			continue
		}
	}
	return result
}

func (c *CRClient) GetAndLease(ctx context.Context, timelineID []byte, buckets domain.BucketRange, consumerId string, leaseMs uint64, limit int, maxTimestamp uint64) ([]timeline.Lease, bool, error) {

	//bs := make([]int, 0, buckets.Size())
	//if buckets.Start < buckets.End {
	//	for b := buckets.Start; b < buckets.End; b++ {
	//		bs = append(bs, int(b))
	//	}
	//} else {
	//	for b := buckets.End; b < buckets.Start; b++ {
	//		bs = append(bs, int(b))
	//	}
	//}
	//bucketIDsHolders := strings.Split(strings.Repeat("?", buckets.Size()), "")

	//https://www.cockroachlabs.com/docs/v19.2/update.html#update-and-return-values
	query := fmt.Sprintf(`UPDATE %s SET timeline = MAX(:now, timeline) + :lease, consumer_id = :consumerid WHERE 
		( timeline <= :now OR (timeline > :now AND timeline <= :maxts AND consumer_id = NUL )) 
		AND bucket_id >= :bmin AND bucket_id <= :bmax
		LIMIT :limit
		RETURNING 'id';
	`, table(string(timelineID)))
	rows, err := c.db.QueryContext(ctx, query,
		sql.Named("now", dtime.TimeToMS(time.Now())),
		sql.Named("lease", leaseMs),
		sql.Named("consumerid", []byte(consumerId)),
		sql.Named("maxts", maxTimestamp),
		sql.Named("bmin", buckets.Min()),
		sql.Named("bmax", buckets.Max()),
		sql.Named("limit", limit),
	)
	if err != nil {
		return nil, false, fmt.Errorf("failed preparing stmt %w", err)
	}

	// Print out the returned value.
	defer rows.Close()
	msgIDs := [][]byte{}
	for rows.Next() {
		var id []byte
		if err := rows.Scan(&id); err != nil {
			log.Fatal(err)
		}
		msgIDs = append(msgIDs, id)
	}

	result := make([]timeline.Lease, 0, len(msgIDs))

	query = fmt.Sprintf(`SELECT * FROM %s AS topic INNER JOIN %s ON id = topic.body_id WHERE topic.ID IN ();`,
		table(string(timelineID)), tableBodies(string(timelineID)))

	c.db.QueryContext(ctx, query)

	return result, len(result) == limit, nil
}

func (c *CRClient) Lookup(ctx context.Context, timelineID []byte, messageIDs [][]byte) ([]timeline.Message, []errors.MessageIDTuple) {
	panic("implement me")
}

func (c *CRClient) Delete(ctx context.Context, timelineID []byte, messageIDs []timeline.Message) []errors.MessageIDTuple {
	batchSize := 25
	var batch [][]byte
	result := make([]errors.MessageIDTuple, 0)
	ids := make([][]byte, len(messageIDs))
	for i := range messageIDs {
		ids[i] = messageIDs[i].ID
	}

	for len(ids) > 0 {
		max := min(len(ids), batchSize)
		batch = ids[:max]
		ids = ids[max:]

		holders := strings.Split(strings.Repeat("?", max), "")
		query := fmt.Sprintf(`DELETE * FROM %s WHERE id IN (%s);`,
			table(string(timelineID)), strings.Join(holders, ","))

		args := make([]interface{}, len(batch))
		for i := range batch {
			args[i] = batch[i]
		}
		_, err := c.db.ExecContext(ctx, query, args...)
		if err != nil {
			for i := range batch {
				result = append(result, errors.MessageIDTuple{
					MessageID: batch[i],
					Error: errors.Dejaror{
						Severity:         errors.SeverityError,
						Message:          "failed",
						Module:           errors.ModuleStorage,
						Operation:        "Delete",
						Kind:             0,
						Details:          nil,
						WrappedErr:       err,
						ShouldRetry:      true,
						ClientShouldSync: false,
					},
				})
			}
		}
	}

	return result
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
