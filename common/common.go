package common

import "unsafe"

//TODO WIP


type TimelineMessage struct {
	ID []byte
	TimestampMSUTC uint64
	BodyID []byte
	ProducerGroupID []byte
	LockConsumerID []byte
	BucketID uint16
	BodySize uint32
	Version uint16
}

func (m TimelineMessage) GetID() string {
	return *(*string)(unsafe.Pointer(&m.ID))
}
func (m TimelineMessage) GetProducerGroupID() string {
	return *(*string)(unsafe.Pointer(&m.ProducerGroupID))
}
func (m TimelineMessage) GetLockConsumerID() string {
	return *(*string)(unsafe.Pointer(&m.LockConsumerID))
}

type TopicType uint8
const (
	TopicType_Timeline TopicType = iota
	TopicType_ProrityQueue TopicType = 1
	TopicType_Crons  TopicType = 2
)

type TopicProvisioningStatus uint8
const (
	TopicProvisioningStatus_Creating = iota
	TopicProvisioningStatus_Live = 2
	TopicProvisioningStatus_Deleting= 3
)

type TimelineTopic struct {
	Name string
	CreationTimestamp uint64

	//The state of a topic
	ProvisionStatus TopicProvisioningStatus
	BrokerIDMaster string

	//ShardID (the timeslice offset can have multiple shards to avoid, here we keep the state
	TimeWindowShards map[uint64]uint16


	//lag (msg to be processed, the oldest and the count)
	AproxMsgCountAvailable uint64
	OldestTimestamp uint64

	Settings TimelineSettings
}

type TimelineSettings struct {
	//TODO add all settings, and find a way to differentiate the mutable ones
	ReplicaCount int
	//The number of seconds, compared to Now(), that a message is allowed to be set on the timeline. How far in the future is allowed, eg: 1year. 0 for infinity.
	MaxSecondsFutureAllowed uint64 //default 1year
	//Largest duration allowed for a lease.
	MaxSecondsLease uint64 //default 5min
	ChecksumBodies bool //default false
	MaxBodySizeBytes uint64 //default 1mb
	//requests per seconds from a specific client
	RQSLimitPerClient uint64 //default 128

	//when a breaking change or new option is added to Timeline logic, this can enforce a minimum client version.
	MinimumProtocolVersion uint16
	MinimumDriverVersion uint16

	BucketCount uint32 //default 1024
}

type Client struct {
	ID string
	IP string
	IPv6 string
	Hostname string
}

type WriteConsistency uint8
const (
	WriteConsistency_Master = iota
	WriteConsistency_Quorum = 1
	WriteConsistency_AllReplicas = 2
	WriteConsistency_FireForget = 3
)


type Error interface {
	Error() string
	String() string
	Code() uint16
	Message() string
	Details() map[string]string
	ThrottledMs() uint16
}