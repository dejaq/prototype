package common

//TODO WIP


type TimelineMessage struct {
	ID []byte
	Timestamp uint64
	BodyID []byte
	BucketID uint16
	BodySize uint32
	Version uint16
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
	BrokerIDMaster string
}

type TimelineTopicSynchronization struct {
	Shards uint16
	AproxMsgCount uint64
	OldestTimestamp uint64
}