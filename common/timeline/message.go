package timeline

import (
	"unsafe"
)

// Message is a timeline full representation. Not all the fields are populated all the time
type Message struct {
	//An UUID, unique across the topic, as string, non-canonical form, without hypes and stored as bytes
	ID []byte
	// the Unix timestamp, in milliseconds, when the message should be processed
	TimestampMS uint64
	// the id of the payload
	BodyID []byte
	// the payload
	Body []byte
	// Group of producers that has ownership, string
	ProducerGroupID []byte
	// Unique consumer client id that has a lock on it. Only valid if TimestampMS >= Now()
	LockConsumerID []byte
	// Randomly chosen at creation, used by the Loader
	BucketID uint16
	// Updated at each mutation, used to detect mutation contention
	Version uint16
}

func (m Message) GetID() string {
	return *(*string)(unsafe.Pointer(&m.ID))
}

func (m Message) GetBodyID() string {
	return *(*string)(unsafe.Pointer(&m.BodyID))
}

func (m Message) GetBody() string {
	return *(*string)(unsafe.Pointer(&m.Body))
}

func (m Message) GetProducerGroupID() string {
	return *(*string)(unsafe.Pointer(&m.ProducerGroupID))
}

func (m Message) GetLockConsumerID() string {
	return *(*string)(unsafe.Pointer(&m.LockConsumerID))
}

func (m Message) String() string {
	return "{Message [id:" + m.GetID() + "]"
}

type DeleteCaller uint8

const (
	DeleteCallerConsumer DeleteCaller = iota
	DeleteCallerProducer
)

type DeleteMessagesRequest struct {
	// Reference timestamp for calculate active leases
	Timestamp uint64
	// Who wants to delete messages
	CallerType DeleteCaller
	//Identity of who wants to delete
	CallerID   string
	TimelineID string
	Messages   []DeleteMessagesRequestItem
}

func (dm *DeleteMessagesRequest) GetTimelineID() string {
	return *(*string)(unsafe.Pointer(&dm.TimelineID))
}

type DeleteMessagesRequestItem struct {
	MessageID []byte
	BucketID  uint16
	Version   uint16
}

func (r DeleteMessagesRequestItem) GetMessageID() string {
	return *(*string)(unsafe.Pointer(&r.MessageID))
}

type InsertMessagesRequest struct {
	TimelineID string
	// Group of producers that has ownership, string
	ProducerGroupID string
	Messages        []InsertMessagesRequestItem
}

func (m InsertMessagesRequest) GetProducerGroupID() string {
	return m.ProducerGroupID
}

func (m InsertMessagesRequest) GetTimelineID() string {
	return m.TimelineID
}

// InsertMessagesRequestItem one msg that should be inserted
type InsertMessagesRequestItem struct {
	//An UUID, unique across the topic, as string, non-canonical form, without hypes and stored as bytes
	ID []byte
	// the Unix timestamp, in milliseconds, when the message should be processed
	TimestampMS uint64
	// the payload
	Body []byte
	// Randomly chosen at creation, used by the Loader
	BucketID uint16
	// Updated at each mutation, used to detect mutation contention
	Version uint16
}

func (m InsertMessagesRequestItem) GetID() string {
	return *(*string)(unsafe.Pointer(&m.ID))
}

func (m InsertMessagesRequestItem) GetBody() string {
	return *(*string)(unsafe.Pointer(&m.Body))
}

func (m InsertMessagesRequestItem) String() string {
	return "{InsertMessagesRequestItem [id:" + m.GetID() + "]"
}

// MessageStatus represents a transient state of a message as time goes by see the docs design-docs/09.timeline.md
type MessageStatus uint8

const (
	// (Timestamp <= NOW)
	StatusAvailable MessageStatus = iota
	// Waiting status (new messages) (LockConsumerID is empty)
	StatusWaiting
	// Processing status (it has a Lease) (LockConsumerID not empty)
	StatusProcessing
)

type CountRequest struct {
	TimelineID string
	Type       MessageStatus
	//BothInclusive and Optional (see time.IsZero() for emtpy value)
	//Min, Max time.Time
}
