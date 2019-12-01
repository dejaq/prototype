package timeline

import (
	"unsafe"

	"github.com/bgadrian/dejaq-broker/common/protocol"
)

// Message is a timeline full representation. Not all the fields are populated all the time
type Message struct {
	//An UUID, unique across the topic, as string, non-canonical form, without hypes and stored as bytes
	ID []byte
	// the Unix timestamp, in milliseconds, when the message should be processed
	TimestampMS uint64
	// the ID of the payload
	BodyID []byte
	// the payload
	Body []byte
	// Group of producers that has ownership, string
	ProducerGroupID []byte
	// Unique consumer client ID that has a lock on it. Only valid if TimestampMS >= Now()
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
	return "{Message [ID:" + m.GetID() + "]"
}

type Topic struct {
	ID                string
	CreationTimestamp uint64

	//The state of a topic
	ProvisionStatus protocol.TopicProvisioningStatus
	BrokerIDMaster  string

	//ShardID (the timeslice offset can have multiple shards to avoid, here we keep the state
	TimeWindowShards map[uint64]uint16

	//lag (msg to be processed, the oldest and the count)
	AproxMsgCountAvailable uint64
	OldestTimestamp        uint64

	Settings TopicSettings
}

type TopicSettings struct {
	//TODO add all settings, and find a way to differentiate the mutable ones
	ReplicaCount int32
	//The number of seconds, compared to Now(), that a message is allowed to be set on the timeline. How far in the future is allowed, eg: 1year. 0 for infinity.
	MaxSecondsFutureAllowed uint64 //default 1year
	//Largest duration allowed for a lease.
	MaxSecondsLease  uint64 //default 5min
	ChecksumBodies   bool   //default false
	MaxBodySizeBytes uint64 //default 1mb
	//requests per seconds from a specific client
	RQSLimitPerClient uint64 //default 128

	//when a breaking change or new option is added to Timeline logic, this can enforce a minimum client version.
	MinimumProtocolVersion uint16
	MinimumDriverVersion   uint16

	BucketCount uint16 //default 1024
}

//TODO see if these gets generated by the flat buffers

type Lease struct {
	ExpirationTimestampMS uint64
	ConsumerID            []byte
	Message               LeaseMessage
}

func (l Lease) GetConsumerID() string {
	return *(*string)(unsafe.Pointer(&l.ConsumerID))
}

// LeaseMessage is the message representation received by a consumer
type LeaseMessage struct {
	//An UUID, unique across the topic, as string, non-canonical form, without hypes and stored as bytes
	ID []byte
	// the Unix timestamp, in milliseconds, when the message should be processed
	TimestampMS uint64
	// Group of producers that has ownership, string
	ProducerGroupID []byte
	// Updated at each mutation, used to detect mutation contention
	Version uint16
	// the payload
	Body     []byte
	BucketID uint16
}

func (m LeaseMessage) GetID() string {
	return *(*string)(unsafe.Pointer(&m.ID))
}
func (m LeaseMessage) GetProducerGroupID() string {
	return *(*string)(unsafe.Pointer(&m.ProducerGroupID))
}

func (m LeaseMessage) String() string {
	return "{Message [ID:" + m.GetID() + "]"
}

func NewLeaseMessage(msg Message) LeaseMessage {
	return LeaseMessage{
		ID:              msg.ID,
		TimestampMS:     msg.TimestampMS,
		ProducerGroupID: msg.ProducerGroupID,
		Version:         msg.Version,
		Body:            msg.Body,
		BucketID:        msg.BucketID,
	}
}
