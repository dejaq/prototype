package cockroach

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
	"unsafe"

	storageTimeline "github.com/dejaq/prototype/broker/pkg/storage/timeline"

	"github.com/dejaq/prototype/broker/domain"
	derrors "github.com/dejaq/prototype/common/errors"
	dtime "github.com/dejaq/prototype/common/time"
	"github.com/dejaq/prototype/common/timeline"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

var (
	stmtTopic = `
CREATE TABLE  IF NOT EXISTS "$TOPIC" (
	id STRING NOT NULL,
	timeline INT NOT NULL,
	bucket_id INT NOT NULL,    
	ts INT NOT NULL,
	producer_group_id STRING NOT NULL,
	consumer_id STRING,
	body_id STRING NOT NULL,
	version INT NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (id ASC),
	/* used by GetAndLease */
	INDEX $TOPIC_index_timeline_bucket (timeline, bucket_id ASC),
	/*used by SelectByConsumerID */
	INDEX $TOPIC_index_consumerID_bucket (consumer_id, bucket_id ASC),    
	FAMILY stable (id, ts, bucket_id, producer_group_id, body_id),  
 	FAMILY mutable (timeline, consumer_id, version)
);

CREATE TABLE IF NOT EXISTS "$BODY"  (
	id STRING NOT NULL,
	body STRING,
	CONSTRAINT "primary" PRIMARY KEY (id ASC)
);
`
) //https://www.cockroachlabs.com/docs/stable/foreign-key.html

var _ = storageTimeline.Repository(&CRClient{})

type CRClient struct {
	db     *sql.DB
	logger logrus.FieldLogger
}

func New(db *sql.DB, logger logrus.FieldLogger) *CRClient {
	return &CRClient{db: db, logger: logger.WithField("component", "cockroach")}
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
		return c.externalErr(ctx, err, "createTopic")
	}
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (c *CRClient) Insert(ctx context.Context, req timeline.InsertMessagesRequest) error {
	batchSize := 25
	var batch []timeline.InsertMessagesRequestItem
	result := make(derrors.MessageIDTupleList, 0)

	addFailedBatch := func(err error) {
		c.logger.WithError(err).Error("insert msg batch failed")
		for i := range batch {
			result = append(result, derrors.MessageIDTuple{
				MsgID: batch[i].ID,
				MsgError: derrors.Dejaror{
					Severity:         derrors.SeverityError,
					Message:          "failed",
					Module:           derrors.ModuleStorage,
					Operation:        "Insert",
					Kind:             0,
					Details:          nil,
					WrappedErr:       nil,
					ShouldRetry:      true,
					ClientShouldSync: false,
				},
			})
		}
	}

	messages := make([]timeline.InsertMessagesRequestItem, len(req.Messages))
	for i := range req.Messages {
		messages[i] = req.Messages[i]
	}

	for len(messages) > 0 {
		max := min(len(messages), batchSize)
		batch = messages[:max]
		messages = messages[max:]

		txn, err := c.db.Begin()
		if err != nil {
			addFailedBatch(err)
			continue
		}

		//https://godoc.org/github.com/lib/pq#hdr-Bulk_imports
		stmt, err := txn.PrepareContext(ctx, pq.CopyIn(table(req.GetTimelineID()),
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
				batch[i].GetID(),
				batch[i].BucketID,
				batch[i].TimestampMS,
				batch[i].TimestampMS,
				req.GetProducerGroupID(),
				batch[i].GetID(), //body_id same as msg_id
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

		stmtBodies, err := txn.PrepareContext(ctx, pq.CopyIn(tableBodies(req.GetTimelineID()), "id", "body"))
		if err != nil {
			addFailedBatch(err)
			txn.Rollback()
			continue
		}
		for i := range batch {
			_, err = stmtBodies.Exec(
				batch[i].GetID(),
				//TODO check if we can use RawBytes to remove memory allocations on the driver side
				batch[i].GetBody())
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
	queryUpdate := strings.Replace(`UPDATE "$TOPIC" SET timeline = GREATEST($1, timeline) + $2, consumer_id = $3 WHERE 
		( timeline <= $1 OR (timeline > $1 AND timeline <= $4 AND consumer_id = NULL )) 
		AND bucket_id IN ($BUCKETS)
		LIMIT $5
		RETURNING id;
	`, "$TOPIC", table(string(timelineID)), -1)
	queryUpdate = strings.Replace(queryUpdate, "$BUCKETS", buckets.CSV(), 1)
	//Note: the server will automatically do some retries https://www.cockroachlabs.com/docs/stable/transactions.html#automatic-retries

	st, err := c.db.PrepareContext(ctx, queryUpdate)
	if err != nil {
		return nil, false, err
	}

	consumerStr := *(*string)(unsafe.Pointer(&consumerId))
	rows, err := st.Query(
		dtime.TimeToMS(time.Now()), leaseMs, consumerStr,
		maxTimestamp, limit,
		//TODO make the named parameter work :/ or file a bug report
		//sql.Named("now", dtime.TimeToMS(time.Now())),
		//sql.Named("lease", leaseMs),
		//sql.Named("consumerid", []byte(consumerId)),
		//sql.Named("maxts", maxTimestamp),
		//sql.Named("limit", limit),
	)
	if err != nil {
		return nil, false, c.externalErr(ctx, err, "getAndLease")
	}
	defer rows.Close()

	// Print out the returned value.
	msgIDs := []interface{}{}
	params := []string{}
	//var ids string
	//var idb []byte
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, false, c.externalErr(ctx, err, "getAndLease")
		}
		//https://www.cockroachlabs.com/docs/v19.2/bytes.html#main-content
		msgIDs = append(msgIDs, id)
		//msgIDs = append(msgIDs, fmt.Sprintf("\\x%x", id))
		//for SQL parameters $1, $2 ...
		params = append(params, fmt.Sprintf("$%d", len(msgIDs)))
		//ids = fmt.Sprintf("%x", id)
		//idb = id
	}

	//c.logger.Infof("%v %v", ids, idb)

	if len(msgIDs) == 0 {
		return nil, false, nil
	}

	//TODO Prepare does notwork in IN statements, do a manual escape for sql injections
	querySelect := fmt.Sprintf(`SELECT 
		topic.id, topic.timeline, topic.bucket_id, topic.ts, topic.producer_group_id, topic.version, bodies.body
		FROM "%s" AS topic INNER JOIN "%s" AS bodies ON bodies.id = topic.body_id WHERE topic.id IN (%s);`,
		table(string(timelineID)), tableBodies(string(timelineID)), strings.Join(params, ","))

	sel, err := c.db.PrepareContext(ctx, querySelect)
	if err != nil {
		//TODO revert the leases
		return nil, false, c.externalErr(ctx, err, "getAndLease")
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
		var id string
		var producerID string
		var body string
		//TODO add here RawBytes and read the body without a copy! optimization, then copy the slice to the lease.message.body
		if err := rowsSelect.Scan(
			&id, &lease.ExpirationTimestampMS, &lease.Message.BucketID, &lease.Message.TimestampMS, &producerID, &lease.Message.Version, &body,
		); err != nil {
			//TODO revert the leases
			return nil, true, c.externalErr(ctx, err, "getAndLease")
		}
		lease.Message.ID = []byte(id)
		lease.Message.ProducerGroupID = []byte(producerID)
		lease.ConsumerID = consumerId
		lease.Message.Body = []byte(body)
		result = append(result, lease)
	}

	if len(result) != len(msgIDs) {
		//TODO revert the leases
		return result, true, c.externalErr(ctx, err, "getAndLease")
	}
	//we do not know if there are more rows, just assume
	return result, len(result) == limit, nil
}

func (c *CRClient) Delete(ctx context.Context, request timeline.DeleteMessagesRequest) error {
	batchSize := 25
	var batch []string
	result := make(derrors.MessageIDTupleList, 0)
	ids := make([]string, len(request.Messages))
	for i := range request.Messages {
		ids[i] = request.Messages[i].GetMessageID()
	}

	failBatch := func(err error) {
		c.logger.WithError(err).Error("delete batch failed")
		for i := range batch {
			result = append(result, derrors.MessageIDTuple{
				MsgID: *(*[]byte)(unsafe.Pointer(&batch[i])),
				MsgError: derrors.Dejaror{
					Severity:         derrors.SeverityError,
					Message:          "failed",
					Module:           derrors.ModuleStorage,
					Operation:        "Delete",
					Kind:             0,
					Details:          nil,
					WrappedErr:       nil,
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

		//questionMarks := generateQuestionMarks(max)
		idsAsText := make([]interface{}, len(batch))
		params := make([]string, len(batch))
		for i := range batch {
			idsAsText[i] = batch[i]
			params[i] = fmt.Sprintf("$%d", i+1)
		}
		//the same queries works for bodies as well

		//TODO Prepare does notwork in IN statements, do a manual escape for sql injections
		queryPattern := fmt.Sprintf(`DELETE FROM "$TABLE" WHERE id IN (%s) LIMIT %d RETURNING NOTHING;`, strings.Join(params, ","), len(batch))

		//args := make([]interface{}, len(batch))
		//for i := range batch {
		//	args[i] = batch[i]
		//}

		txn, err := c.db.Begin()
		if err != nil {
			failBatch(err)
			continue
		}
		query := strings.Replace(queryPattern, "$TABLE", table(request.GetTimelineID()), -1)
		stmt, err := txn.PrepareContext(ctx, query)
		if err != nil {
			c.logger.Debugf(query)
			failBatch(err)
			txn.Rollback()
			continue
		}
		_, err = stmt.ExecContext(ctx, idsAsText...)
		if err != nil {
			c.logger.Debugf(query)
			failBatch(err)
			txn.Rollback()
			continue
		}

		query = strings.Replace(queryPattern, "$TABLE", tableBodies(request.GetTimelineID()), -1)
		stmt, err = txn.PrepareContext(ctx, query)
		if err != nil {
			c.logger.Debugf(query)
			failBatch(err)
			txn.Rollback()
			continue
		}
		_, err = stmt.ExecContext(ctx, idsAsText...)
		if err != nil {
			c.logger.Debugf(query)
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

func generateIncParams(start, end int) string {
	params := []string{}
	for i := start; i <= end; i++ {
		params = append(params, fmt.Sprintf("$%d", i))
	}
	return strings.Join(params, ",")
}

func (c *CRClient) SelectByConsumer(ctx context.Context, timelineID []byte, consumerID []byte, buckets domain.BucketRange, limit int, maxTimestamp uint64) ([]timeline.Lease, bool, error) {
	return nil, false, nil
}

func (c *CRClient) externalErr(ctx context.Context, sql error, operation string) derrors.Dejaror {
	//keep the implementation details from leaving the storage context
	c.logger.WithError(sql).Error("sql failed")

	//TODO calculate this and other scenarios
	var isTimeout bool
	return derrors.Dejaror{
		Severity:         derrors.SeverityError,
		Message:          "failed",
		Module:           derrors.ModuleStorage,
		Operation:        derrors.Op(operation),
		Kind:             0,
		Details:          nil,
		WrappedErr:       nil,
		ShouldRetry:      isTimeout,
		ClientShouldSync: false,
	}
}
