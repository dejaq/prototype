package cockroach

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"unsafe"

	storageTimeline "github.com/dejaq/prototype/broker/pkg/storage/timeline"

	"github.com/dejaq/prototype/broker/domain"
	derrors "github.com/dejaq/prototype/common/errors"
	"github.com/dejaq/prototype/common/timeline"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

var (
	stmtTopic = `
CREATE TABLE  IF NOT EXISTS "$TOPIC" (
	/* we leverage the ORDERED keys in ranges from cockroachDB and sort the items such 
	to make the LeaseUpdates optimal. bucketIDs are zero padded left to be sorted lexicographically */
	bucketid_msgid STRING NOT NULL,
	id STRING NOT NULL,
	timeline INT NOT NULL,
	bucket_id INT NOT NULL,    
	ts INT NOT NULL,
	producer_group_id STRING NOT NULL,
	consumer_id STRING,
	body STRING,
	version INT NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (bucketid_msgid ASC),
	/* used by GetAndLease , the bucketID is in there */
	INDEX $TOPIC_index_timeline_bucket (bucketid_msgid, timeline ASC), 
	/*used by SelectByConsumerID */
	INDEX $TOPIC_index_consumerID_bucket (consumer_id, bucket_id ASC),   
	FAMILY stable (bucketid_msgid, id, ts, bucket_id, producer_group_id, body),  
 	FAMILY mutable (timeline, consumer_id, version)
);
`

	TOIMPLEMENT = `
CREATE TABLE IF NOT EXISTS "v2_ts_$TOPIC" (
	/*bucketid_unixmsTS_msgid */
	tsid STRING NOT NULL,
	consumer_id STRING,
	/* used for leases */
	CONSTRAINT "primary" PRIMARY KEY (tsid ASC),
	/* used by preloading */
	INDEX $TOPIC_index_with_cid (tsid, consumer_id ASC),
	/* used by Hydrate */
	INDEX $TOPIC_INDEX_consumer_id (consumer_id ASC),
);

Insert - both tables
Delete - both tables
Lease: 
//get the available messages from my bucket 29, max TS (now) and the maximum msgID (ÿ=255byte largest character)
//buketID and TS have leading padding zeros
DELETE FROM FROM v2_ts WHERE tsid < 029_001234433112+1 LIMIT 10 RETURNING ID;
//move them up to the timeline
INSERT INTO v2_ts (new tsids .... + consumerID);
//hope we will hit as few as possible different ranges as they are grouped by bucket_id
SELECT * FROM v2_meta WHERE bucket_id_msg_id IN ( .... );


CREATE TABLE  IF NOT EXISTS "v2_meta_$TOPIC" (
	bucket_id_msg_id STRING NOT NULL,
	id STRING NOT NULL,
	timeline INT NOT NULL,
	bucket_id INT NOT NULL,    
	ts INT NOT NULL,
	producer_group_id STRING NOT NULL,
	consumer_id STRING,
	body STRING,
	version INT NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (bucket_id_msg_id ASC),
	/* used by GetAndLease */
	INDEX $TOPIC_index_timeline_bucket (timeline, bucket_id ASC),
	/*used by SelectByConsumerID */
	INDEX $TOPIC_index_consumerID_bucket (consumer_id, bucket_id ASC),    
	FAMILY stable (id, ts, bucket_id, producer_group_id, body),  
 	FAMILY mutable (timeline, consumer_id, version)
);
`
)

var _ = storageTimeline.Repository(&CRClient{})

type CRClient struct {
	db                 *sql.DB
	logger             logrus.FieldLogger
	InsertBatchMaxSize int
	DeleteBatchMaxSize int
}

func New(db *sql.DB, logger logrus.FieldLogger) *CRClient {
	return &CRClient{db: db, logger: logger.WithField("component", "cockroach")}
}

func table(topicID string) string {
	return fmt.Sprintf("dejaq_timeline_%s", topicID)
}

func (c *CRClient) CreateTopic(ctx context.Context, timelineID string) error {
	//table and column names cannot be prepared
	replaced := strings.Replace(stmtTopic, "$TOPIC", table(timelineID), -1)
	_, err := c.db.ExecContext(ctx, replaced)
	if err != nil {
		return c.externalErr(ctx, err, "createTopic")
	}
	c.logger.Infof("created topic=%s", timelineID)
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func padBucketID(bucketID uint16) string {
	return fmt.Sprintf("%05d", bucketID)
}
func timelineMsgID(bucketID uint16, msgID string) string {
	return padBucketID(bucketID) + "_" + msgID
}

func (c *CRClient) Insert(ctx context.Context, req timeline.InsertMessagesRequest) error {
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
		max := min(len(messages), c.InsertBatchMaxSize)
		batch = messages[:max]
		messages = messages[max:]

		txn, err := c.db.Begin()
		if err != nil {
			addFailedBatch(err)
			continue
		}

		//https://godoc.org/github.com/lib/pq#hdr-Bulk_imports
		stmt, err := txn.PrepareContext(ctx, pq.CopyIn(table(req.GetTimelineID()),
			"bucketid_msgid",
			"id",
			"bucket_id",
			"ts",
			"timeline",
			"producer_group_id",
			"body",
			"version"))
		if err != nil {
			addFailedBatch(err)
			txn.Rollback()
			continue
		}

		var failedSt bool
		for i := range batch {
			_, err = stmt.Exec(
				timelineMsgID(batch[i].BucketID, batch[i].GetID()),
				batch[i].GetID(),
				batch[i].BucketID,
				batch[i].TimestampMS,
				batch[i].TimestampMS,
				req.GetProducerGroupID(),
				batch[i].GetBody(),
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

		err = txn.Commit()
		if err != nil {
			addFailedBatch(err)
			txn.Rollback()
			continue
		}
	}
	return result
}

type leaseQuery struct {
	ctx                context.Context
	queryUpdate        string
	consumerId         []byte
	leaseDurationMS    uint64
	maxTimestamp       uint64
	limit              int
	timelineID         []byte
	minInclusiveBucket uint16
	maxExclusiveBucket uint16
}

func (c *CRClient) GetAndLease(ctx context.Context, timelineID []byte, buckets domain.BucketRange, consumerId []byte, leaseMs uint64, limit int, currentTimeMS uint64, maxTimestamp uint64) ([]timeline.Lease, bool, error) {
	//OPTIMISTIC we make a query for available messages first , is faster

	//https://www.cockroachlabs.com/docs/v19.2/update.html#update-and-return-values
	queryUpdate := strings.Replace(`UPDATE "$TOPIC" SET timeline = GREATEST($1, timeline) + $2, consumer_id = $3 WHERE 
		 bucketid_msgid >= $4 AND bucketid_msgid < $5 AND timeline <= $1 
		LIMIT $6
		RETURNING bucketid_msgid;
	`, "$TOPIC", table(string(timelineID)), -1)
	//Note: the server will automatically do some retries https://www.cockroachlabs.com/docs/stable/transactions.html#automatic-retries

	availableLeases, hasMore, err := c.leaseByQuery(leaseQuery{
		ctx:             ctx,
		queryUpdate:     queryUpdate,
		consumerId:      consumerId,
		leaseDurationMS: leaseMs,
		// notice the currentTimeMS !!!! this means only available (from the past TS) will be returned
		maxTimestamp:       currentTimeMS,
		limit:              limit,
		timelineID:         timelineID,
		minInclusiveBucket: buckets.Min(),
		maxExclusiveBucket: buckets.Max() + 1,
	})
	if err != nil || len(availableLeases) >= limit {
		//if there were enough available messages we return these
		return availableLeases, hasMore, err
	}

	//TODO make this work
	////there were not enough leases! it means we are searching in the future for preloading!
	//queryUpdate = strings.Replace(`UPDATE "$TOPIC" SET timeline = GREATEST($1, timeline) + $2, consumer_id = $3 WHERE
	//	( timeline <= $1 OR (timeline > $1 AND timeline <= $4 AND consumer_id = NULL ))
	//	AND bucket_id IN ($BUCKETS)
	//	LIMIT $5
	//	RETURNING id;
	//`, "$TOPIC", table(string(timelineID)), -1)
	//queryUpdate = strings.Replace(queryUpdate, "$BUCKETS", bucketsAsCSV, 1)
	////Note: the server will automatically do some retries https://www.cockroachlabs.com/docs/stable/transactions.html#automatic-retries
	//
	//futureLeases, _, err := c.leaseByQuery(ctx, queryUpdate, consumerId, leaseMs, maxTimestamp, limit-len(availableLeases), timelineID)
	//
	//return append(availableLeases, futureLeases...), hasMore, err
	return availableLeases, hasMore, err
}

func (c *CRClient) leaseByQuery(request leaseQuery) ([]timeline.Lease, bool, error) {
	st, err := c.db.PrepareContext(request.ctx, request.queryUpdate)
	if err != nil {
		return nil, false, err
	}

	consumerStr := *(*string)(unsafe.Pointer(&request.consumerId))
	rows, err := st.Query(
		request.maxTimestamp,                    //$1
		request.leaseDurationMS,                 //$2
		consumerStr,                             //$3
		padBucketID(request.minInclusiveBucket), //$4
		padBucketID(request.maxExclusiveBucket), //$5
		request.limit,                           //$6
		//TODO make the named parameter work :/ or file a bug report
		//sql.Named("now", dtime.TimeToMS(time.Now())),
		//sql.Named("lease", leaseDurationMS),
		//sql.Named("consumerid", []byte(consumerId)),
		//sql.Named("maxts", maxTimestamp),
		//sql.Named("limit", limit),
	)
	if err != nil {
		return nil, false, c.externalErr(request.ctx, err, "getAndLease")
	}
	defer rows.Close()

	//TODO warning! if the method fails from here on or the broker dies, the messages will remain locked with a lease

	// Print out the returned value.
	msgIDs := []interface{}{}
	params := []string{}
	//var ids string
	//var idb []byte
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, false, c.externalErr(request.ctx, err, "getAndLease")
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
		topic.id, topic.timeline, topic.bucket_id, topic.ts, topic.producer_group_id, topic.version, topic.body
		FROM "%s" AS topic WHERE topic.bucketid_msgid IN (%s);`,
		table(string(request.timelineID)), strings.Join(params, ","))

	sel, err := c.db.PrepareContext(request.ctx, querySelect)
	if err != nil {
		//TODO revert the leases
		return nil, false, c.externalErr(request.ctx, err, "getAndLease")
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
			return nil, true, c.externalErr(request.ctx, err, "getAndLease")
		}
		lease.Message.ID = []byte(id)
		lease.Message.ProducerGroupID = []byte(producerID)
		lease.ConsumerID = request.consumerId
		lease.Message.Body = []byte(body)
		result = append(result, lease)
	}

	if len(result) != len(msgIDs) {
		//TODO revert the leases
		return result, true, c.externalErr(request.ctx, err, "getAndLease")
	}
	//we do not know if there are more rows, just assume
	return result, len(result) == request.limit, nil
}

func (c *CRClient) Delete(ctx context.Context, request timeline.DeleteMessagesRequest) error {
	var batch []string
	result := make(derrors.MessageIDTupleList, 0)
	ids := make([]string, len(request.Messages))
	for i := range request.Messages {
		ids[i] = timelineMsgID(request.Messages[i].BucketID, request.Messages[i].GetMessageID())
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
		max := min(len(ids), c.DeleteBatchMaxSize)
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

		//TODO Prepare does not work in IN statements, do a manual escape for sql injections
		queryPattern := fmt.Sprintf(`DELETE FROM "$TABLE" WHERE bucketid_msgid IN (%s) LIMIT %d RETURNING NOTHING;`, strings.Join(params, ","), len(batch))

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
