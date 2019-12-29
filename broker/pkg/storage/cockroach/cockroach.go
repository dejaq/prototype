package cockroach

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/dejaq/prototype/broker/domain"
	"github.com/dejaq/prototype/common/errors"
	dtime "github.com/dejaq/prototype/common/time"
	"github.com/dejaq/prototype/common/timeline"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

var (
	stmtTopic = `
DROP TABLE IF EXISTS $TOPIC CASCADE;
CREATE TABLE $TOPIC (
	id BYTES NOT NULL,
	timeline INT NOT NULL,
	bucket_id INT NOT NULL,    
	ts INT NOT NULL,
	producer_group_id BYTES NOT NULL,
	consumer_id BYTES,
	body_id BYTES NOT NULL,
	version INT NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (id ASC),
	INDEX $TOPIC_index_ts (ts ASC),
	INDEX $TOPIC_index_bucket (bucket_id ASC),
	INDEX $TOPIC_index_consumerID (consumer_id ASC),    
	FAMILY stable (id, ts, bucket_id, producer_group_id, body_id),  
 	FAMILY mutable (timeline, consumer_id, version)
		);
DROP TABLE IF EXISTS $BODY CASCADE;
CREATE TABLE $BODY (
	id BYTES NOT NULL,
	body BYTES,
	CONSTRAINT "primary" PRIMARY KEY (id ASC)
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
	return fmt.Sprintf("dejaq_timeline_%s", topicID)
}
func tableBodies(topicID string) string {
	return fmt.Sprintf("dejaq_timeline_%s_bodies", topicID)
}

func (c *CRClient) CreateTopic(ctx context.Context, timelineID string) error {
	//table and column names cannot be prepared
	replaced := strings.Replace(stmtTopic, "$TOPIC", table(timelineID), -1)
	replaced = strings.Replace(replaced, "$BODY", tableBodies(timelineID), -1)
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

		txn, err := c.db.Begin()
		if err != nil {
			log.Fatal(err)
		}

		//https://godoc.org/github.com/lib/pq#hdr-Bulk_imports
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
			txn.Rollback()
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
		}

		if failedSt {
			addFailedBatch(err)
			txn.Rollback()
			continue
		}
		err = stmt.Close()
		if err != nil {
			addFailedBatch(err)
			txn.Rollback()
			continue
		}

		stmtBodies, err := txn.Prepare(pq.CopyIn(tableBodies(string(timelineID)), "id", "body"))
		if err != nil {
			addFailedBatch(err)
			txn.Rollback()
			continue
		}
		for i := range batch {
			_, err = stmtBodies.Exec(
				batch[i].ID,
				//TODO check if we can use RawBytes to remove memory allocations on the driver side
				batch[i].Body)
			if err != nil {
				failedSt = true
				break
			}
		}

		if failedSt {
			addFailedBatch(err)
			txn.Rollback()
			continue
		}

		err = stmtBodies.Close()
		if err != nil {
			addFailedBatch(err)
			txn.Rollback()
			continue
		}

		err = txn.Commit()
		if err != nil {
			addFailedBatch(err)
			txn.Rollback()
			continue
		}
	}
	return result
}

func (c *CRClient) GetAndLease(ctx context.Context, timelineID []byte, buckets domain.BucketRange, consumerId []byte, leaseMs uint64, limit int, currentTimeMS uint64, maxTimestamp uint64) ([]timeline.Lease, bool, error) {

	//https://www.cockroachlabs.com/docs/v19.2/update.html#update-and-return-values
	queryUpdate := strings.Replace(`UPDATE $TOPIC SET timeline = GREATEST($1, timeline) + $2, consumer_id = $3 WHERE 
		( timeline <= $1 OR (timeline > $1 AND timeline <= $4 AND consumer_id = NULL )) 
		AND bucket_id >= $5 AND bucket_id <= $6
		LIMIT $7
		RETURNING id;
	`, "$TOPIC", table(string(timelineID)), -1)

	st, err := c.db.PrepareContext(ctx, queryUpdate)
	if err != nil {
		return nil, false, err
	}
	rows, err := st.Query(
		dtime.TimeToMS(time.Now()), leaseMs, consumerId, maxTimestamp, buckets.Min(), buckets.Max(), limit,
		//TODO make the named parameter work :/ or file a bug report
		//sql.Named("now", dtime.TimeToMS(time.Now())),
		//sql.Named("lease", leaseMs),
		//sql.Named("consumerid", []byte(consumerId)),
		//sql.Named("maxts", maxTimestamp),
		//sql.Named("bmin", buckets.Min()),
		//sql.Named("bmax", buckets.Max()),
		//sql.Named("limit", limit),
	)
	if err != nil {
		return nil, false, fmt.Errorf("failed preparing stmt %w", err)
	}
	defer rows.Close()

	// Print out the returned value.
	msgIDs := []interface{}{}
	params := []string{}
	//var ids string
	//var idb []byte
	for rows.Next() {
		var id sql.RawBytes
		if err := rows.Scan(&id); err != nil {
			log.Fatal(err)
		}
		//https://www.cockroachlabs.com/docs/v19.2/bytes.html#main-content
		msgIDs = append(msgIDs, id)
		//for SQL parameters $1, $2 ...
		params = append(params, fmt.Sprintf("$%d", len(msgIDs)))
		//ids = fmt.Sprintf("%x", id)
		//idb = id
	}

	//c.logger.Infof("%v %v", ids, idb)

	if len(msgIDs) == 0 {
		return nil, false, nil
	}

	querySelect := fmt.Sprintf(`SELECT 
		topic.id, topic.timeline, topic.bucket_id, topic.ts, topic.producer_group_id,
		topic.consumer_id, topic.version, bodies.body
		FROM %s AS topic INNER JOIN %s AS bodies ON bodies.id = topic.body_id WHERE topic.id IN (%s);`,
		table(string(timelineID)), tableBodies(string(timelineID)), strings.Join(params, ","))

	sel, err := c.db.PrepareContext(ctx, querySelect)
	if err != nil {
		//TODO revert the leases
		return nil, false, err
	}
	defer sel.Close()

	rowsSelect, err := sel.Query(msgIDs...)
	if err != nil {
		//TODO revert the leases
		return nil, false, err
	}
	defer rowsSelect.Close()

	result := make([]timeline.Lease, 0, len(msgIDs))
	for rowsSelect.Next() {
		lease := timeline.Lease{}
		//TODO add here RawBytes and read the body without a copy! optimization, then copy the slice to the lease.message.body
		if err := rowsSelect.Scan(
			&lease.Message.ID, &lease.ExpirationTimestampMS, &lease.Message.BucketID, &lease.Message.TimestampMS, &lease.Message.ProducerGroupID,
			&lease.ConsumerID, &lease.Message.Version, &lease.Message.Body,
		); err != nil {
			log.Fatal(err)
		}
		if bytes.Compare(lease.ConsumerID, consumerId) != 0 {
			c.logger.Fatalf("consumerID is different  %v %v", lease.ConsumerID, consumerId)
		}
		result = append(result, lease)
	}

	if len(result) != len(msgIDs) {
		//TODO revert the leases
		return result, true, errors.NewDejaror("missing leased IDs (BUG)", "fetch")
	}
	//we do not know if there are more rows, just assume
	return result, len(result) == limit, nil
}

func (c *CRClient) Lookup(ctx context.Context, timelineID []byte, messageIDs [][]byte) ([]timeline.Message, []errors.MessageIDTuple) {
	panic("implement me")
}

func (c *CRClient) Delete(ctx context.Context, deleteMessages timeline.DeleteMessages) []errors.MessageIDTuple {
	batchSize := 25
	var batch [][]byte
	result := make([]errors.MessageIDTuple, 0)
	ids := make([][]byte, len(deleteMessages.Messages))
	for i := range deleteMessages.Messages {
		ids[i] = deleteMessages.Messages[i].MessageID
	}

	failBatch := func(err error) {
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

	for len(ids) > 0 {
		max := min(len(ids), batchSize)
		batch = ids[:max]
		ids = ids[max:]

		holders := strings.Split(strings.Repeat("?", max), "")
		//the same queries works for bodies as well
		query := fmt.Sprintf(`DELETE * FROM $TABLE WHERE id IN (%s);`, strings.Join(holders, ","))

		args := make([]interface{}, len(batch))
		for i := range batch {
			args[i] = batch[i]
		}

		txn, err := c.db.Begin()
		if err != nil {
			failBatch(err)
			continue
		}
		_, err = txn.ExecContext(ctx, strings.Replace(query, "$TABLE", table(string(deleteMessages.TimelineID)), -1), args...)
		if err != nil {
			failBatch(err)
			txn.Rollback()
			continue
		}
		_, err = txn.ExecContext(ctx, strings.Replace(query, "$TABLE", tableBodies(string(deleteMessages.TimelineID)), -1), args...)
		if err != nil {
			failBatch(err)
			txn.Rollback()
			continue
		}

		err = txn.Commit()
		if err != nil {
			failBatch(err)
			txn.Rollback()
			continue
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

func (c *CRClient) SelectByConsumer(ctx context.Context, timelineID []byte, consumerID []byte, buckets domain.BucketRange, timeReferenceMS uint64) []timeline.Message {
	panic("implement me")
}

func (c *CRClient) SelectByProducer(ctx context.Context, timelineID []byte, producrID []byte) []timeline.Message {
	panic("implement me")
}
