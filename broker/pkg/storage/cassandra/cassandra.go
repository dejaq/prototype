package cassandra

import (
	"context"
	"errors"
	"fmt"
	"unsafe"

	"github.com/dejaq/prototype/broker/domain"

	storage "github.com/dejaq/prototype/broker/pkg/storage/timeline"
	derrors "github.com/dejaq/prototype/common/errors"
	"github.com/dejaq/prototype/common/timeline"
	"github.com/gocql/gocql"
)

const (
	tableNameMetadata = "metadata"
)

var (
	stmtInsertTimeline          = "INSERT INTO timeline_%s (msgID, timestamp, consumerID, producerGroupID, bodyID, size, version) VALUES (?, ?, ?, ?, ?, ?)"
	stmtInsertMessage           = fmt.Sprintf("INSERT INTO %s (msgID, body) VALUES (?, ?)", tableNameMetadata)
	stmtSelectAvailableMessages = "SELECT msgID FROM timeline_%s WHERE timestamp >= ? AND timestamp <= ? AND consumerID='' LIMIT ?"
	stmtSelectMessagesCount     = "SELECT count(*) FROM timeline_%s WHERE timestamp >= ? AND timestamp <= ?"
	stmtSelectMessagesMetadata  = "SELECT msgID, timestamp, consumerID, producerGroupID, size, version FROM timeline_%s WHERE msgID IN ?"
	stmtSelectMessages          = "SELECT msgID, body FROM %s WHERE msgID=?"
	stmtDeleteMessages          = "DELETE FROM %s WHERE msgID=?"
)

type Metadata struct {
	ID              []byte
	BucketID        uint16
	TimestampMS     uint64
	Size            uint64
	BodyID          []byte
	ProducerGroupID []byte
	LockConsumerID  []byte
	Version         uint16
}

func (m Metadata) GetID() string {
	return *(*string)(unsafe.Pointer(&m.ID))
}
func (m Message) GetProducerGroupID() string {
	return *(*string)(unsafe.Pointer(&m.ProducerGroupID))
}
func (m Message) GetLockConsumerID() string {
	return *(*string)(unsafe.Pointer(&m.LockConsumerID))
}

func getUnsafeString(a []byte) string {
	return *(*string)(unsafe.Pointer(&a))
}

type Message struct {
	*Metadata
	Body []byte
}

//force implementation of interface
var _ = storage.Repository(&Cassandra{})

type Cassandra struct {
	clusterConfig *gocql.ClusterConfig
	session       *gocql.Session
}

func NewCassandra(hosts []string) *Cassandra {
	return &Cassandra{
		clusterConfig: gocql.NewCluster(hosts...),
	}
}

func (w *Cassandra) Init() error {
	session, err := gocql.NewSession(*w.clusterConfig)
	if err != nil {
		return err
	}
	w.session = session
	return nil
}

// INSERT messages (timelineID, []messages) map[msgID]error
func (w *Cassandra) Insert(ctx context.Context, timelineID []byte, messages []timeline.Message) []derrors.MessageIDTuple {
	batch := w.session.NewBatch(gocql.LoggedBatch)
	batch.WithContext(ctx)
	for _, msg := range messages {
		batch.Query(stmtInsertMessage, msg.ID, msg.BodyID)
		batch.Query(fmt.Sprintf(stmtInsertTimeline, getUnsafeString(timelineID)), // TODO replace with method better suited for byte arrays
			msg.ID, msg.TimestampMS, msg.LockConsumerID, msg.ProducerGroupID, len(msg.Body), msg.Version)
	}

	err := w.session.ExecuteBatch(batch)
	if err != nil {

	}
	return nil
}

// SELECT message.id BY TimelineID, BucketIDs ([]bucketIDs, limit, maxTimestamp) ([]messages, hasMore, error)
func (w *Cassandra) Select(ctx context.Context, timelineID []byte, buckets []domain.BucketRange, limit int, maxTimestamp uint64) ([]timeline.Message, bool, error) {
	return nil, false, nil
}

// UPDATE timestamp BY TimelineID, MessageIDs (map[msgID]newTimestamp])  map[msgID]error (for extend/release)
func (w *Cassandra) Update(ctx context.Context, timelineID []byte, messageTimestamps []storage.MsgTime) []derrors.MessageIDTuple {
	return nil
}

func (w *Cassandra) UpdateLeases(ctx context.Context, timelineID []byte, msgs []timeline.Message) []derrors.MessageIDTuple {
	return nil
}

// LOOKUP message by TimelineID, MessageID (owner control, lease operations)
func (w *Cassandra) Lookup(ctx context.Context, timelineID []byte, messageIDs [][]byte) ([]timeline.Message, []derrors.MessageIDTuple) {
	return nil, nil
}

// DELETE messages by TimelineID, MessageID map[msgID]error
func (w *Cassandra) Delete(ctx context.Context, timelineID []byte, messageIDs []timeline.Message) []derrors.MessageIDTuple {
	return nil
}

// COUNT messages BY TimelineID, RANGE (spike detection/consumer scaling and metrics)
func (w *Cassandra) CountByRange(ctx context.Context, timelineID []byte, a, b uint64) uint64 {
	return 0
}

// COUNT messages BY TimelineID, RANGE and LockConsumerID is empty (count processing status)
func (w *Cassandra) CountByRangeProcessing(ctx context.Context, timelineID []byte, a, b uint64) uint64 {
	return 0
}

// COUNT messages BY TimelineID, RANGE and LockConsumerID is not empty (count waiting status)
func (w *Cassandra) CountByRangeWaiting(ctx context.Context, timelineID []byte, a, b uint64) uint64 {
	return 0
}

// SELECT messages by TimelineID, LockConsumerID (when consumer restarts)
func (w *Cassandra) SelectByConsumer(ctx context.Context, timelineID []byte, consumerID []byte) []timeline.Message {
	return nil
}

// SELECT messages by TimelineID, ProducerOwnerID (ownership control)
func (w *Cassandra) SelectByProducer(ctx context.Context, timelineID []byte, producrID []byte) []timeline.Message {
	return nil
}

func (w *Cassandra) GetAvailableMessages(ctx context.Context, timeline string, from, until float64, limit int) (ids []string, err error) {
	var id string

	iter := w.session.Query(fmt.Sprintf(stmtSelectAvailableMessages, timeline), from, until, limit).WithContext(ctx).Iter()

	for iter.Scan(&id) {
		ids = append(ids, id)
	}

	err = iter.Close()

	return ids, err
}

func (w *Cassandra) GetMessagesMetadata(ctx context.Context, timelineID []byte, ids []string) (msgs map[string]Metadata, err error) {
	var id []byte
	var timestamp uint64
	var consumerID []byte
	var producerGroupID []byte
	var bodyID []byte
	var size uint64
	var version uint16

	msgs = make(map[string]Metadata, len(ids))

	iter := w.session.Query(fmt.Sprintf(stmtSelectMessagesMetadata, timelineID), ids).WithContext(ctx).Iter()

	for iter.Scan(&id, &timestamp, &consumerID, &producerGroupID, &bodyID, &size, &version) {
		msgs[getUnsafeString(id)] = Metadata{
			ID:              id,
			TimestampMS:     timestamp,
			LockConsumerID:  consumerID,
			ProducerGroupID: producerGroupID,
			BodyID:          bodyID,
			Size:            size,
			Version:         version,
		}
	}

	err = iter.Close()

	return msgs, err
}

func (w *Cassandra) GetMessagesBody(ctx context.Context, timelineID []byte, ids []string) (msgs map[string][]byte, err error) {
	var id string
	var body []byte

	msgs = make(map[string][]byte, len(ids))

	iter := w.session.Query(fmt.Sprintf(stmtSelectMessagesMetadata, string(timelineID)), ids).WithContext(ctx).Iter()

	for iter.Scan(&id, &body) {
		msgs[id] = body
	}

	err = iter.Close()

	return msgs, err
}

func (w *Cassandra) GetMessages(ctx context.Context, timelineID []byte, ids []string) ([]timeline.Message, error) {
	msgsMetadata, err := w.GetMessagesMetadata(ctx, timelineID, ids)
	if err != nil {
		return nil, err
	}
	msgsBody, err := w.GetMessagesBody(ctx, timelineID, ids)
	if err != nil {
		return nil, err
	}
	if len(msgsMetadata) != len(msgsBody) {
		return nil, errors.New(fmt.Sprintf("invalid number of entities found: ids=%d, metadata=%d, body=%d", len(ids), len(msgsMetadata), len(msgsBody)))
	}
	messages := make([]timeline.Message, 0, len(ids))
	for msgID, meta := range msgsMetadata {
		messages = append(messages, timeline.Message{
			ID:              meta.ID,
			TimestampMS:     meta.TimestampMS,
			BodyID:          meta.BodyID,
			ProducerGroupID: meta.ProducerGroupID,
			LockConsumerID:  meta.LockConsumerID,
			BucketID:        meta.BucketID,
			Version:         meta.Version,
			Body:            msgsBody[msgID],
		})
	}

	return messages, nil
}

func (w *Cassandra) GetMessagesByConsumerID(ctx context.Context, timeline string, consumerID string) ([]string, error) {
	return nil, nil
}

func (w *Cassandra) GetMessagesByProducerID(ctx context.Context, timeline string, producerID string) ([]string, error) {
	return nil, nil
}

func (w *Cassandra) DeleteMessages(ctx context.Context, timeline string, ids []string) error {
	for _, id := range ids {
		err := w.DeleteMessage(ctx, timeline, id)
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *Cassandra) DeleteMessage(ctx context.Context, timeline string, id string) error {
	batch := w.session.NewBatch(gocql.LoggedBatch)
	batch.WithContext(ctx)
	batch.Query(fmt.Sprintf(stmtDeleteMessages, timeline), id)
	batch.Query(fmt.Sprintf(stmtDeleteMessages, tableNameMetadata), id)

	return w.session.ExecuteBatch(batch)
}

func (w *Cassandra) GetMessagesCountByConsumerID(ctx context.Context, timeline string, from, until float64, consumerID *string, producerID *string) (count int64, err error) {
	var values []interface{}
	stmt := fmt.Sprintf(stmtSelectMessagesCount, timeline)

	if consumerID != nil {
		stmt = fmt.Sprintf("%s AND consumerID = ? ", stmt)
		values = append(values, *consumerID)
	}
	if producerID != nil {
		stmt = fmt.Sprintf("%s AND producerID = ? ", stmt)
		values = append(values, *producerID)
	}
	if len(values) == 0 {
		return 0, errors.New("no filter specified")
	}
	err = w.session.Query(stmt, values...).Scan(&count)
	return count, err
}

//CREATE  KEYSPACE IF NOT EXISTS dejaq
//WITH REPLICATION = {
//'class' : 'SimpleStrategy', 'replication_factor' : 2 }
//}
//AND DURABLE_WRITES =  true;

//CREATE TABLE dejaq.metadata (
//id text,
//timeline_id text,
//bucket_id text,
//timestampMS int,
//size int,
//body_id text
//producer_group_id text
//lock_consumer_id text
//version int
//PRIMARY KEY ((timeline_id, bucket_id), id)
//);

//CREATE TABLE dejaq.message (
//timeline_id text,
//bucket_id text,
//body_id text,
//body blob,
//PRIMARY KEY ((timeline_id, bucket_id), id)
//);
